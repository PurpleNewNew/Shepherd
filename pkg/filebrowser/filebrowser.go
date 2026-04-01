package filebrowser

import "encoding/json"

// Entry 描述一个远端目录项。
type Entry struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	IsDir      bool   `json:"is_dir,omitempty"`
	IsDrive    bool   `json:"is_drive,omitempty"`
	IsSymlink  bool   `json:"is_symlink,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Mode       string `json:"mode,omitempty"`
	ModifiedAt string `json:"modified_at,omitempty"`
	Hidden     bool   `json:"hidden,omitempty"`
}

// Listing 描述一次结构化目录浏览结果。
type Listing struct {
	RequestedPath string  `json:"requested_path,omitempty"`
	ResolvedPath  string  `json:"resolved_path"`
	DisplayPath   string  `json:"display_path,omitempty"`
	RootPath      string  `json:"root_path"`
	ParentPath    string  `json:"parent_path,omitempty"`
	CanGoUp       bool    `json:"can_go_up,omitempty"`
	VirtualRoot   bool    `json:"virtual_root,omitempty"`
	Entries       []Entry `json:"entries,omitempty"`
}

func Marshal(listing Listing) ([]byte, error) {
	return json.Marshal(listing)
}

func Unmarshal(data []byte) (Listing, error) {
	var listing Listing
	err := json.Unmarshal(data, &listing)
	return listing, err
}
