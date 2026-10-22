package main

import (
	"context"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------
// REAL DOWNLOAD ENGINE (actually saves files to disk)
// ---------------------------------------------------

type ActiveDownload struct {
	mu        sync.Mutex
	Item      *DownloadItem
	Cancelled bool
	cancel    context.CancelFunc
}

var activeDownloads = struct {
	mu    sync.Mutex
	Items map[int]*ActiveDownload
}{Items: make(map[int]*ActiveDownload)}

var activeWG sync.WaitGroup

// StartDownload kicks off a real HTTP download in a background goroutine
func StartDownload(rawURL string, saveDir string) *DownloadItem {
	item := downloadManager.Start(rawURL)
	snapshot := *item

	ctx, cancel := context.WithCancel(context.Background())
	active := &ActiveDownload{Item: item, cancel: cancel}
	activeDownloads.mu.Lock()
	activeDownloads.Items[item.ID] = active
	activeDownloads.mu.Unlock()

	activeWG.Add(1)
	go func() {
		defer activeWG.Done()
		defer cancel()
		err := doDownload(ctx, rawURL, resolveSaveDir(saveDir), item.ID, snapshot.FileName, active)
		if err != nil {
			if _, cancelled := err.(errDownloadCancelled); cancelled {
				downloadManager.Cancel(item.ID)
			} else {
				active.mu.Lock()
				wasCancelled := active.Cancelled
				active.mu.Unlock()
				if wasCancelled {
					downloadManager.Cancel(item.ID)
				} else {
					downloadManager.Fail(item.ID)
				}
			}
			cleanupActiveDownload(item.ID)
			return
		}
		downloadManager.Complete(item.ID)
		cleanupActiveDownload(item.ID)
	}()

	return &snapshot
}

// resolveSaveDir turns the saved setting ("Downloads" by default) into a real folder
func resolveSaveDir(saveDir string) string {
	saveDir = strings.TrimSpace(saveDir)
	if filepath.IsAbs(saveDir) {
		return saveDir
	}
	if dir, err := userDownloadsDirectory(); err == nil {
		if saveDir == "" || strings.EqualFold(saveDir, "Downloads") {
			return dir
		}
		if home, herr := os.UserHomeDir(); herr == nil {
			return filepath.Join(home, saveDir)
		}
		return dir
	}
	if saveDir == "" {
		return "Downloads"
	}
	return saveDir
}

func doDownload(ctx context.Context, rawURL string, saveDir string, id int, fallbackName string, active *ActiveDownload) (err error) {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 SPARROW")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errNotOK(resp.StatusCode)
	}

	if err := os.MkdirAll(saveDir, 0755); err != nil {
		return err
	}

	name := fallbackName
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, perr := mime.ParseMediaType(cd); perr == nil {
			if fn := strings.TrimSpace(params["filename"]); fn != "" {
				name = path.Base(strings.ReplaceAll(fn, "\\", "/"))
			}
		}
	}
	fullPath, out, err := createUniqueFile(saveDir, sanitizeFileName(name))
	if err != nil {
		return err
	}
	downloadManager.SetFile(id, filepath.Base(fullPath), fullPath)

	closed := false
	defer func() {
		if !closed {
			out.Close()
		}
		if err != nil {
			os.Remove(fullPath) // never leave partial files behind
		}
	}()

	total := resp.ContentLength
	var written int64
	buf := make([]byte, 32*1024)
	lastPercent := -1

	for {
		active.mu.Lock()
		cancelled := active.Cancelled
		active.mu.Unlock()
		if cancelled {
			return errDownloadCancelled{}
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := out.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			written += int64(n)
			if total > 0 {
				percent := int((written * 100) / total)
				if percent != lastPercent {
					lastPercent = percent
					downloadManager.UpdateProgress(id, percent)
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	closed = true
	if err = out.Close(); err != nil {
		return err
	}
	downloadManager.UpdateProgress(id, 100)
	return nil
}

// createUniqueFile creates name, or name (1).ext, name (2).ext ... if it exists
func createUniqueFile(dir string, name string) (string, *os.File, error) {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	candidate := name
	for i := 1; i < 10000; i++ {
		full := filepath.Join(dir, candidate)
		f, err := os.OpenFile(full, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err == nil {
			return full, f, nil
		}
		if !os.IsExist(err) {
			return "", nil, err
		}
		candidate = base + " (" + strconv.Itoa(i) + ")" + ext
	}
	return "", nil, os.ErrExist
}

func cleanupActiveDownload(id int) {
	activeDownloads.mu.Lock()
	delete(activeDownloads.Items, id)
	activeDownloads.mu.Unlock()
}

// CancelActiveDownload stops the transfer and deletes the partial file
func CancelActiveDownload(id int) {
	activeDownloads.mu.Lock()
	active, ok := activeDownloads.Items[id]
	activeDownloads.mu.Unlock()

	if !ok {
		downloadManager.Cancel(id)
		return
	}
	active.mu.Lock()
	active.Cancelled = true
	active.mu.Unlock()
	if active.cancel != nil {
		active.cancel()
	}
}

// ShutdownDownloads is called when the app closes: stops every transfer,
// removes partial files and wipes the whole download list.
func ShutdownDownloads() {
	activeDownloads.mu.Lock()
	for _, active := range activeDownloads.Items {
		active.mu.Lock()
		active.Cancelled = true
		active.mu.Unlock()
		if active.cancel != nil {
			active.cancel()
		}
	}
	activeDownloads.mu.Unlock()

	done := make(chan struct{})
	go func() { activeWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
	downloadManager.ClearAll()
}

type errDownloadCancelled struct{}

func (errDownloadCancelled) Error() string { return "download cancelled" }

func sanitizeFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		name = "download"
	}
	replacer := strings.NewReplacer(
		"<", "_", ">", "_", ":", "_", "\"", "_",
		"/", "_", "\\", "_", "|", "_", "?", "_", "*", "_",
	)
	return replacer.Replace(name)
}

type downloadHTTPError struct {
	code int
}

func (e *downloadHTTPError) Error() string {
	return "download failed with status code"
}

func errNotOK(code int) error {
	return &downloadHTTPError{code: code}
}

// IsDownloadableURL checks file extensions that should trigger a download
// instead of opening in the browser (basic heuristic)
func IsDownloadableURL(rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	lower := strings.ToLower(parsed.Path)
	downloadExts := []string{
		".zip", ".rar", ".7z", ".exe", ".msi", ".dmg",
		".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
		".mp3", ".mp4", ".mkv", ".avi", ".mov",
		".iso", ".apk", ".tar", ".gz",
	}
	for _, ext := range downloadExts {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}
