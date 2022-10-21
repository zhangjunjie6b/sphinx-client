[![Go Report Card](https://goreportcard.com/badge/github.com/zhangjunjie6b/sphinx-client)](https://goreportcard.com/report/github.com/zhangjunjie6b/sphinx-client)


# 背景

任职的老项目组还在使用sphinx，并且我发现在go里面没有sphinx的client，毕竟2022年了！

这是一个基于php翻写的项目，实现了大多常用的功能。如果在使用中遇见任何问题欢迎lssue或者pr

我没有太多的精力去深度使用和测试，如果遇见了非预期的结果可以向我反馈我会及时修复。

## 安装 
```
go get github.com/zhangjunjie6b/sphinx-client
```

## 事例
```go
	s := sphinx.New()
	s.SetServer("10.111.196.154",3312)
	s.SetConnTimeout(1)
	s.SetMatchMode(sphinx.SPH_MATCH_ANY)
	//s.SetSortMode(sphinx.SPH_SORT_EXTENDED, "pr desc")
	//s.SetArrayResult(true)
	s.SetFieldWeights([]sphinx.Fieldweights{{"title", 999},{"keyword",100}})
	//s.SetFilter("c1", []int{1}, true)
	s.SetFilterRange("picid_i", uint(881642), uint(881645), false)
	req,err := s.Query("美女", "sphinx_search_newliulan sphinx_main_search_proxy", "")
```