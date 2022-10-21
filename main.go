package main

import (
	"fmt"
	"github.com/zhangjunjie6b/sphinx-client/sphinx"
)

func main()  {

	s := sphinx.New()
	s.SetServer("10.111.196.154",3312)



	req,err := s.Query("海报美容PPT模板", "sphinx_search_newliulan sphinx_main_search_proxy", "")

	if err != nil {
		fmt.Println(any(err))
	}

	fmt.Println(req)


	req,err = s.Query("sfdkwsadmas;ldasdsadas", "sphinx_search_newliulan sphinx_main_search_proxy", "")

	if err != nil {
		fmt.Println(any(err))
	}

	fmt.Println(req)



}
