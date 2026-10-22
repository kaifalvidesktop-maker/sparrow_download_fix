package main

import (
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------
// DOWNLOAD TRACKER (RAM ONLY - LIST CLEARS ON RESTART)
// ---------------------------------------------------

type DownloadItem struct {
	ID         int
	FileName   string
	Path       string // full path of the saved file
	URL        string
	Progress   int    // 0 to 100
	Status     string // "downloading", "completed", "failed", "cancelled"
	StartedAt  time.Time
	FinishedAt time.Time
}

type DownloadManager struct {
	mu     sync.Mutex
	Items  []*DownloadItem
	NextID int
}

var downloadManager = &DownloadManager{
	Items:  []*DownloadItem{},
	NextID: 1,
}

// Start tracking a new download
func (dm *DownloadManager) Start(rawURL string) *DownloadItem {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	fileName := fileNameFromURL(rawURL)

	item := &DownloadItem{
		ID:        dm.NextID,
		FileName:  fileName,
		URL:       rawURL,
		Progress:  0,
		Status:    "downloading",
		StartedAt: time.Now(),
	}

	dm.Items = append(dm.Items, item)
	dm.NextID++

	return item
}

// SetFile updates the final file name and path once they are known
func (dm *DownloadManager) SetFile(id int, name string, fullPath string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	for _, item := range dm.Items {
		if item.ID == id {
			item.FileName = name
			item.Path = fullPath
			break
		}
	}
}

func fileNameFromURL(rawURL string) string {
	name := ""
	if u, err := url.Parse(strings.TrimSpace(rawURL)); err == nil {
		name = path.Base(u.Path)
		if decoded, derr := url.PathUnescape(name); derr == nil {
			name = decoded
		}
	}
	if name == "" || name == "." || name == "/" {
		name = "unknown_file"
	}
	return name
}

// Update progress percentage of a download
func (dm *DownloadManager) UpdateProgress(id int, progress int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if progress > 100 {
		progress = 100
	}
	if progress < 0 {
		progress = 0
	}

	for _, item := range dm.Items {
		if item.ID == id {
			item.Progress = progress
			break
		}
	}
}

// Mark a download as completed
func (dm *DownloadManager) Complete(id int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for _, item := range dm.Items {
		if item.ID == id && item.Status == "downloading" {
			item.Status = "completed"
			item.Progress = 100
			item.FinishedAt = time.Now()
			break
		}
	}
}

// Mark a download as failed
func (dm *DownloadManager) Fail(id int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for _, item := range dm.Items {
		if item.ID == id && item.Status == "downloading" {
			item.Status = "failed"
			item.FinishedAt = time.Now()
			break
		}
	}
}

// Cancel an in-progress download
func (dm *DownloadManager) Cancel(id int) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for _, item := range dm.Items {
		if item.ID == id && item.Status == "downloading" {
			item.Status = "cancelled"
			item.FinishedAt = time.Now()
			break
		}
	}
}

// Remove all completed/failed/cancelled downloads from the list
func (dm *DownloadManager) ClearFinished() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	active := []*DownloadItem{}
	for _, item := range dm.Items {
		if item.Status == "downloading" {
			active = append(active, item)
		}
	}
	dm.Items = active
}

func (dm *DownloadManager) ClearAll() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.Items = nil
	dm.NextID = 1
}

// Get all downloads (active + finished)
func (dm *DownloadManager) GetAll() []*DownloadItem {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	result := make([]*DownloadItem, len(dm.Items))
	for i, item := range dm.Items {
		copy := *item
		result[i] = &copy
	}
	return result
}

func (dm *DownloadManager) Import(items []*DownloadItem) int {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	existing := make(map[string]struct{}, len(dm.Items))
	usedIDs := make(map[int]struct{}, len(dm.Items))
	for _, item := range dm.Items {
		if item == nil {
			continue
		}
		existing[downloadImportKey(item)] = struct{}{}
		usedIDs[item.ID] = struct{}{}
	}
	if dm.NextID < 1 {
		dm.NextID = 1
	}

	imported := 0
	for _, item := range items {
		if item == nil || strings.TrimSpace(item.URL) == "" {
			continue
		}
		key := downloadImportKey(item)
		if _, found := existing[key]; found {
			continue
		}
		copy := *item
		if copy.ID < 1 {
			copy.ID = dm.NextID
		}
		if _, found := usedIDs[copy.ID]; found {
			copy.ID = dm.NextID
			for {
				if _, found := usedIDs[copy.ID]; !found {
					break
				}
				copy.ID++
			}
		}
		if copy.ID >= dm.NextID {
			dm.NextID = copy.ID + 1
		}
		if copy.Progress < 0 {
			copy.Progress = 0
		} else if copy.Progress > 100 {
			copy.Progress = 100
		}
		switch copy.Status {
		case "completed", "failed", "cancelled", "downloading":
		default:
			copy.Status = "failed"
		}
		if copy.Status == "downloading" {
			copy.Status = "failed"
			copy.FinishedAt = time.Now()
		}
		dm.Items = append(dm.Items, &copy)
		existing[key] = struct{}{}
		usedIDs[copy.ID] = struct{}{}
		imported++
	}
	return imported
}

func downloadImportKey(item *DownloadItem) string {
	return item.URL + "\x00" + item.FileName + "\x00" + item.StartedAt.UTC().Format(time.RFC3339Nano)
}
