package main

import (
	"fmt"
	"github.com/zhangjunjie6b/sphinx-client/sphinx"
)

func main() {
	s := sphinx.New()
	s.SetServer("127.0.0.1", 3312)
	s.SetConnTimeout(1)
	s.SetMatchMode(sphinx.SPH_MATCH_ANY)
	//s.SetSortMode(sphinx.SPH_SORT_EXTENDED, "pr desc")
	//s.SetArrayResult(true)
	s.SetFieldWeights([]sphinx.Fieldweights{{"title", 999}, {"keyword", 100}})
	//s.SetFilter("c1", []int{1}, true)
	s.SetFilterRange("picid_i", uint(881642), uint(881645), false)
	req, err := s.Query("美女", "name", "")

	fmt.Println(req)

	if err != nil {
		fmt.Println(any(err))
	}
}
