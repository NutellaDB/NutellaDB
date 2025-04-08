package src

import (
	"fmt"
	"os"
)

type Version struct {
	major int
	minor int
	patch int
}

type Header struct {
	version           Version
	page_size         int
	root_page_id      int
	total_pages       int
	free_list_pointer int
}

func ParseHeader(databaseId int) (Header, error) {
	file_ptr, err := os.ReadFile(fmt.Sprintf("./files/%d/header", databaseId))
	if err != nil {
		return Header{}, err
	}

	fmt.Println(string(file_ptr))
	return Header{}, nil
}
