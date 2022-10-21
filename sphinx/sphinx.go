package sphinx

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const (
	// known searchd commands

	SEARCHD_COMMAND_SEARCH = 0
	SEARCHD_COMMAND_EXCERPT= 1
	SEARCHD_COMMAND_UPDATE = 2
	SEARCHD_COMMAND_KEYWORDS = 3

	// current client-side command implementation versions

	VER_COMMAND_SEARCH = "0113"
	VER_COMMAND_EXCERPT = "0x100"
	VER_COMMAND_UPDATE = "0x101"
	VER_COMMAND_KEYWORDS = "0x100"

	// known searchd status codes

	SEARCHD_OK = 0
	SEARCHD_ERROR = 1
	SEARCHD_RETRY = 2
	SEARCHD_WARNING = 3

	// known match modes
	SPH_MATCH_ALL = 0
	SPH_MATCH_ANY = 1
	SPH_MATCH_PHRASE = 2
	SPH_MATCH_BOOLEAN = 3
	SPH_MATCH_EXTENDED = 4
	SPH_MATCH_FULLSCAN = 5
	SPH_MATCH_EXTENDED2 = 6

	// known ranking modes (ext2 only)
	SPH_RANK_PROXIMITY_BM25 = 0
	SPH_RANK_BM25 = 1
	SPH_RANK_NONE = 2
	SPH_RANK_WORDCOUNT = 3

	// known sort modes
	SPH_SORT_RELEVANCE = 0
	SPH_SORT_ATTR_DESC = 1
	SPH_SORT_ATTR_ASC = 2
	SPH_SORT_TIME_SEGMENTS = 3
	SPH_SORT_EXTENDED = 4
	SPH_SORT_EXPR = 5

	//known filter types
	SPH_FILTER_VALUES = 0
	SPH_FILTER_RANGE = 1
	SPH_FILTER_FLOATRANGE = 2

	// known attribute types
	SPH_ATTR_INTEGER = 1
	SPH_ATTR_TIMESTAMP = 2
	SPH_ATTR_ORDINAL = 3
	SPH_ATTR_BOOL = 4
	SPH_ATTR_FLOAT = 5
	SPH_ATTR_MULTI = 0x40000000

	// known grouping functions
	SPH_GROUPBY_DAY = 0
	SPH_GROUPBY_WEEK = 1
	SPH_GROUPBY_MONTH = 2
	SPH_GROUPBY_YEAR = 3
	SPH_GROUPBY_ATTR = 4
	SPH_GROUPBY_ATTRPAIR = 5
)

var(
	ErrNoClient = errors.New("no sphinx node available")
	ErrRetry = errors.New("cannot connect after several retries")
	ErrRetryMessage = errors.New("retry message error")
	ErrTimeout = errors.New("timeout")
	ErrVersions =  errors.New("sphinx service versions no support")
	ErrParameter = errors.New("paramete no support")
)

type Filter struct {
	Type int
	Attr string
	Exclude bool
	Values []int
	Min uint
	Max uint
	Min_float float32
	Max_float float32
}
type Indexweight struct {
	Idx string
	Weight int
}
type Fieldweights struct {
	Name string
	Weight int
}
type vars struct {
	host string
	port int
	offset uint
	limit uint
	mode int
	ranker int
	sort int
	sortby string
	weights []int
	min_id uint
	max_id uint
	filters []Filter
	groupfunc int
	groupby string
	maxmatches uint
	groupsort string
	cutoff uint
	retrycount int
	retrydelay int
	groupdistinct string
	indexweights []Indexweight
	maxquerytime uint
	fieldweights []Fieldweights
	conntimeout int
	arrayresult bool
	warning string
	resq [][]byte
}
type Sphinx struct {
	vars vars
	Conn net.Conn
}

type Result struct {
	Error string
	Warning string
	Status int
	Fields []string
	Attrs  map[string]uint32
	Matches map[interface{}]Matches
	Total uint32
	TotalFound uint32
	Time float32
	Words map[string]Words
}
type Words struct {
	Docs uint32
	Hits uint32
}

type Matches struct {
	Id uint64
	Weight uint32
	Attrs map[interface{}][]interface{}
}

func New() *Sphinx {
	s := Sphinx{
		vars: vars{
			host:          "127.0.0.1",
			port:          3312,
			offset:        0,
			limit:         20,
			mode:          SPH_MATCH_ALL,
			ranker:        SPH_RANK_PROXIMITY_BM25,
			sort:          SPH_SORT_RELEVANCE,
			sortby:        "",
			weights:       nil,
			min_id:        0,
			max_id:        0,
			filters:       nil,
			groupfunc:     SPH_GROUPBY_DAY,
			groupby:       "",
			maxmatches:    1000,
			groupsort:     "@group desc",
			cutoff:        0,
			retrycount:    0,
			retrydelay:    0,
			groupdistinct: "",
			indexweights:  nil,
			maxquerytime:  0,
			fieldweights:  nil,
			conntimeout:   2,
			arrayresult: false,
		},
	}

	return &s
}

func (s *Sphinx) GetConnTimeout() int {
	return  s.vars.conntimeout
}

func (s *Sphinx) SetConnTimeout(timeout int)  {
	s.vars.conntimeout = timeout
}

func (s *Sphinx) SetServer(host string, port int)  {
	s.vars.host = host
	s.vars.port = port
}

func  (s *Sphinx) connect() (net.Conn,error) {
	//1.建立一个链接（Dial拨号
	conn, err := net.DialTimeout("tcp", s.vars.host + ":" + strconv.Itoa(s.vars.port),
		time.Second * time.Duration(s.GetConnTimeout()))

	if err != nil {
		return nil, fmt.Errorf("%w:%s", ErrNoClient,err.Error())
	}

	version := make([]byte, 4)
	io.ReadFull(conn, version)

	if (!bytes.Equal(version, []byte{0x01, 0x00,0x00,0x00})){
		return nil, fmt.Errorf("%w:%s %b", ErrVersions, "Connect response" , version)
	}

	conn.Write([]byte{0x00,0x00,0x00,0x01})
	s.Conn = conn
	return conn,nil
}

func (s *Sphinx) SetLimits (offset uint, limit uint, max uint , cutoff uint)  {
	s.vars.offset = offset
	s.vars.limit = limit
	s.vars.maxmatches = max
	s.vars.cutoff = cutoff
}

func (s *Sphinx) SetMaxQueryTime (max uint) {
	s.vars.maxquerytime = max
}

func (s *Sphinx) SetMatchMode (mode int) error {
	if (mode == SPH_MATCH_ALL || mode == SPH_MATCH_ANY || mode == SPH_MATCH_PHRASE || mode == SPH_MATCH_BOOLEAN ||
		mode == SPH_MATCH_EXTENDED ||
		mode == SPH_MATCH_EXTENDED2 ){
		s.vars.mode = mode
		return nil
	} else {
		return fmt.Errorf("%w:%s",ErrParameter, "SetMatchMode")
	}
}

func (s *Sphinx) SetRankingMode(ranker int) error {
	if (ranker == SPH_RANK_PROXIMITY_BM25 || ranker == SPH_RANK_BM25 || ranker == SPH_RANK_NONE || ranker == SPH_RANK_WORDCOUNT ){
		s.vars.ranker = ranker
		return nil
	} else {
		return fmt.Errorf("%w:%s",ErrParameter, "SetRankingMode")
	}
}

func (s *Sphinx) SetSortMode(mode int, sortby string) error {
	if (mode == SPH_SORT_RELEVANCE || mode == SPH_SORT_ATTR_DESC || mode == SPH_SORT_ATTR_ASC || mode == SPH_SORT_TIME_SEGMENTS ||
		mode == SPH_SORT_EXTENDED ||
		mode == SPH_SORT_EXPR) {
		s.vars.sort = mode
		s.vars.sortby = sortby
		return nil
	}

	return fmt.Errorf("%w:%s",ErrParameter, "SetSortMode")
}

func (s *Sphinx) SetWeights(weights []int)  {
	s.vars.weights = weights
}

func (s *Sphinx) SetFieldWeights(weights Fieldweights)  {
	s.vars.fieldweights[0] = weights
}

func (s *Sphinx) SetIndexWeights(weights Indexweight)  {
	s.vars.indexweights[0] = weights
}

func (s *Sphinx)  SetIDRange (min uint, max uint) error{
	if min >= max {
		return fmt.Errorf("%s, %w: [%d >= %d]","SetIDRange",ErrParameter, min, max)
	} else {
		s.vars.min_id = min
		s.vars.max_id = max
		return nil
	}
}

func (s *Sphinx) SetFileter (attribute string, values []int, exclude bool)  {
	f := Filter{
		Type:    SPH_FILTER_VALUES,
		Attr:    attribute,
		Exclude: exclude,
		Values:  values,
	}
	s.vars.filters[len(s.vars.filters)+1] = f
}

func (s *Sphinx) SetFilterRange(attribute string, min uint, max uint,exclude bool)  error{
	if (min>= max) {
		return fmt.Errorf("%s, %w: [%d >= %d]","SetFilterRange",ErrParameter, min, max)
	}

	f := Filter{
		Type:    SPH_FILTER_RANGE,
		Attr:    attribute,
		Exclude: exclude,
		Min:  min,
		Max: max,
	}
	s.vars.filters[len(s.vars.filters)+1] = f

	return nil
}

func (s *Sphinx) SetFilterFloatRange(attribute string, min float32, max float32,exclude bool) error {

	if (min>= max) {
		return fmt.Errorf("%s, %w: [%d >= %d]","SetFilterFloatRange",ErrParameter, min, max)
	}

	f := Filter{
		Type:    SPH_FILTER_FLOATRANGE,
		Attr:    attribute,
		Exclude: exclude,
		Min_float:  min,
		Max_float: max,
	}

	s.vars.filters[len(s.vars.filters)+1] = f

	return nil
}

func (s *Sphinx) SetGeoAnchor() {
	//TODO 下次一定
}

func (s *Sphinx) SetGroupBy(attribute string, fun int, groupsort string) error{

	if (fun == SPH_GROUPBY_DAY || fun == SPH_GROUPBY_WEEK || fun == SPH_GROUPBY_MONTH || fun == SPH_GROUPBY_YEAR ||
		fun == SPH_GROUPBY_ATTR ||
		fun == SPH_GROUPBY_ATTRPAIR ){
		s.vars.groupby = attribute
		s.vars.groupfunc = fun
		s.vars.groupsort = groupsort
		return nil
	} else  {
		return fmt.Errorf(" %w: %s",ErrParameter, "SetGroupBy")
	}

}

func (s *Sphinx) SetGroupDistinct(attribute string) {
	s.vars.groupdistinct = attribute
}

func (s *Sphinx) SetRetries(count int, delay int) {
	s.vars.retrycount = count
	s.vars.retrydelay = delay
}

func (s *Sphinx) SetArrayResult(arrayresult bool) {
	s.vars.arrayresult = arrayresult
}

func (s *Sphinx) ResetFilters()  {
	s.vars.filters = []Filter{}
}

func (s *Sphinx) ResetGroupBy()  {
	s.vars.groupby = ""
	s.vars.groupfunc = SPH_GROUPBY_DAY
	s.vars.groupsort = "@group desc"
	s.vars.groupdistinct = ""
}

func (s *Sphinx) Query (query string, index string, comment string) (Result,error) {

	conn,err := s.connect()
	if err != nil {
		panic(any(err))
	}
	defer conn.Close()

	s.vars.resq = nil
	s.AddQuery(query, index, comment)
	reqs,err := s.runQueries()
	if err !=nil {
		return Result{},err
	}
	s.vars.warning = reqs[0].Warning
	if (reqs[0].Status == SEARCHD_ERROR) {
		return Result{}, errors.New(reqs[0].Error)
	}

	return reqs[0],nil
}

func (s *Sphinx) runQueries() ([]Result,error) {

	//header
	// 4字节 （(known searchd commands) + （current client-side command implementation versions））

	headr := bytes.NewBuffer([]byte{})
	binary.Write(headr, binary.BigEndian, uint16(SEARCHD_COMMAND_SEARCH))
	command_search,_ := hex.DecodeString(VER_COMMAND_SEARCH)
	binary.Write(headr, binary.BigEndian, command_search)

	resqBuff := bytes.NewBuffer([]byte{})

	for _,v := range s.vars.resq {
		resqBuff.Write(v)
	}

	binary.Write(headr, binary.BigEndian, uint32(len(resqBuff.Bytes())+4))
	nreqs := len(s.vars.resq)
	binary.Write(headr, binary.BigEndian, uint32(nreqs))

	headr.Write(resqBuff.Bytes())

	s.Conn.Write(headr.Bytes())

	response,err :=s.getResponse(VER_COMMAND_SEARCH)

	if err != nil{
		return  nil, err
	}

	//parse response
	max := len(response)
	p := 0
	results := []Result{}

	for ires:=0; ires < nreqs && p < max; ires++ {
		result := Result{Matches: map[interface{}]Matches{},Words: map[string]Words{}}
		// extract status
		status := binary.BigEndian.Uint32(response[p:p+4]); p +=4
		result.Status = int(status)

		if (status != SEARCHD_OK) {

			l:=binary.BigEndian.Uint32(response[p:p+4]); p +=4
			message := response[p:p+int(l)]; p+= int(l)

			if status != SEARCHD_WARNING {
				result.Warning = string(message)
			} else {
				result.Error = string(message)
				results = append(results, result)
				continue
			}

		}


		//read schema
		fields := []string{}
		attrs := map[string]uint32{}
		nfields := binary.BigEndian.Uint32(response[p:p+4]); p+=4

		//fields
		for ; int(nfields) > 0 && p < max; nfields-- {

			l := binary.BigEndian.Uint32(response[p:p+4]); p += 4
			fields = append(fields, string(response[p:p+int(l)]));  p+= int(l)
		}


		result.Fields = fields


		//attrs
		nattrs := binary.BigEndian.Uint32(response[p:p+4]); p+=4

		attrsOrder := []string{}
		for ; int(nattrs) > 0 && p < max; nattrs-- {


			l := binary.BigEndian.Uint32(response[p:p+4]); p += 4
			attr := string(response[p:p+int(l)]); p += int(l)

			t := binary.BigEndian.Uint32(response[p:p+4]); p += 4

			attrsOrder = append(attrsOrder, attr)
			attrs[attr] = t
		}
		result.Attrs = attrs

		// read match count matches

		count := int(binary.BigEndian.Uint32(response[p:p+4])); p += 4
		id64  := int(binary.BigEndian.Uint32(response[p:p+4])); p += 4

		var doc uint64
		var weight uint32

		//matches
		idx := -1
		for ;count >0 && p < max; count--{
			idx++

			if id64 == 1{
				doc = binary.BigEndian.Uint64(response[p:p+8]); p += 8
			} else {
				doc = uint64(binary.BigEndian.Uint32(response[p:p+4])); p += 4
			}

			weight = binary.BigEndian.Uint32(response[p:p+4]); p += 4

			attrvals := map[interface{}][]interface{}{}

			for _,attr := range attrsOrder {
				tp := attrs[attr]
				if tp == SPH_ATTR_FLOAT {
					uval := binary.BigEndian.Uint32(response[p:p+4]); p+=4
					attrvals[attr] =  append(attrvals[attr], uval)
					continue
				}

			val := binary.BigEndian.Uint32(response[p:p+4]); p+=4

				if( (tp & SPH_ATTR_MULTI) > 0 ) {
					attrvals[attr] = []interface{}{}

					for ;val >0 && (p+4) < max; val-- {
						attrv := binary.BigEndian.Uint32(response[p:p+4]); p+=4
						attrvals[attr] = append(attrvals[attr], attrv)
					}

				} else {
					attrvals[attr] = append(attrvals[attr], val)
				}

			}

			// create match entry
			if (s.vars.arrayresult) {
				result.Matches[idx] = Matches{Id:doc, Weight: weight, Attrs: attrvals}
			} else {
				result.Matches[doc] = Matches{Weight: weight, Attrs: attrvals}
			}
		}



		total := binary.BigEndian.Uint32(response[p:p+4]); p += 4
		total_found := binary.BigEndian.Uint32(response[p:p+4]); p += 4
		msecs := float32(binary.BigEndian.Uint32(response[p:p+4])); p += 4
		words := int(binary.BigEndian.Uint32(response[p:p+4])); p += 4

		for ;words >0 && p < max; words-- {
			l := int(binary.BigEndian.Uint32(response[p:p+4])); p += 4
			word := string(response[p:p+l]); p+=l
			docs := binary.BigEndian.Uint32(response[p:p+4]); p += 4
			hits := binary.BigEndian.Uint32(response[p:p+4]); p += 4
			result.Words[word] = Words{
				Docs: docs,
				Hits: hits,
			}
		}

		result.Total = total
		result.TotalFound = total_found
		result.Time = msecs/1000

		results = append(results, result)

	}

	return results,nil
}

func (s *Sphinx) AddQuery(query string, index string, comment string) int {
	//$this->_offset, $this->_limit, $this->_mode, $this->_ranker, $this->_sort
	buff := bytes.NewBuffer([]byte{})
	binary.Write(buff, binary.BigEndian, int32(s.vars.offset))
	binary.Write(buff, binary.BigEndian, int32(s.vars.limit))
	binary.Write(buff, binary.BigEndian, int32(s.vars.mode))
	binary.Write(buff, binary.BigEndian, int32(s.vars.ranker))
	binary.Write(buff, binary.BigEndian, int32(s.vars.sort))
	//$req .= pack ( "N", strlen($this->_sortby) ) . $this->_sortby;
	binary.Write(buff, binary.BigEndian, int32(len(s.vars.sortby)))
	buff.Write([]byte(s.vars.sortby))
	//$req .= pack ( "N", strlen($query) ) . $query
	binary.Write(buff, binary.BigEndian, int32(len(query)))
	buff.Write([]byte(query))

/*	$req .= pack ( "N", count($this->_weights) ); // weights
	foreach ( $this->_weights as $weight )
	$req .= pack ( "N", (int)$weight );*/

	binary.Write(buff, binary.BigEndian, int32(len(s.vars.weights)))
	for _,v := range s.vars.weights {
		binary.Write(buff, binary.BigEndian, int32(v))
	}

	//$req .= pack ( "N", strlen($index) ) . $index; // indexes

	binary.Write(buff, binary.BigEndian, int32(len(index)))
	buff.Write([]byte(index))

	//$req .= pack ( "N", 1 ); // id64 range marker
	binary.Write(buff, binary.BigEndian, int32(1))

	//$req .= sphPack64 ( $this->_min_id ) . sphPack64 ( $this->_max_id ); // id64 range
	binary.Write(buff, binary.BigEndian, int64(s.vars.min_id))
	binary.Write(buff, binary.BigEndian, int64(s.vars.max_id))


	//$req .= pack ( "N", count($this->_filters) );

	binary.Write(buff, binary.BigEndian, int32(len(s.vars.filters)))

	for _,v := range s.vars.filters {
		//$req .= pack ( "N", strlen($filter["attr"]) ) . $filter["attr"];
		binary.Write(buff, binary.BigEndian, int32(len(v.Attr)))
		//$req .= pack ( "N", $filter["type"] );
		binary.Write(buff, binary.BigEndian, int32(v.Type))

		switch v.Type {
		case SPH_FILTER_VALUES:
			//$req .= pack ( "N", count($filter["values"]) );
			binary.Write(buff, binary.BigEndian, int32(len(v.Values)))
			for vv := range v.Values {
				binary.Write(buff, binary.BigEndian, float32(vv))
			}
			break;
		case SPH_FILTER_RANGE:
			//$req .= pack ( "NN", $filter["min"], $filter["max"] );
			binary.Write(buff, binary.BigEndian, int32(v.Min))
			binary.Write(buff, binary.BigEndian, int32(v.Max))
			break;
		case SPH_FILTER_FLOATRANGE:
			//$req .= $this->_PackFloat ( $filter["min"] ) . $this->_PackFloat ( $filter["max"] );
			binary.Write(buff, binary.BigEndian, v.Max_float)
			binary.Write(buff, binary.BigEndian, v.Max_float)
			break;
		default:
			{}
			break;
		}
		if v.Exclude {
			binary.Write(buff, binary.BigEndian, int32(1))
		} else {
			binary.Write(buff, binary.BigEndian, int32(0))
		}

	}

	//	$req .= pack ( "NN", $this->_groupfunc, strlen($this->_groupby) ) . $this->_groupby;
	binary.Write(buff, binary.BigEndian, int32(s.vars.groupfunc))
	binary.Write(buff, binary.BigEndian, int32(len(s.vars.groupby)))
	buff.Write([]byte(s.vars.groupby))

	//$req .= pack ( "N", $this->_maxmatches );
	binary.Write(buff, binary.BigEndian, int32(s.vars.maxmatches))

	//$req .= pack ( "N", strlen($this->_groupsort) ) . $this->_groupsort;
	binary.Write(buff, binary.BigEndian, int32(len(s.vars.groupsort)))
	buff.Write([]byte(s.vars.groupsort))

	//$req .= pack ( "NNN", $this->_cutoff, $this->_retrycount, $this->_retrydelay );
	binary.Write(buff, binary.BigEndian, int32(s.vars.cutoff))
	binary.Write(buff, binary.BigEndian, int32(s.vars.retrycount))
	binary.Write(buff, binary.BigEndian, int32(s.vars.retrydelay))

	//$req .= pack ( "N", strlen($this->_groupdistinct) ) . $this->_groupdistinct;
	binary.Write(buff, binary.BigEndian, int32(len(s.vars.groupdistinct)))
	buff.Write([]byte(s.vars.groupdistinct))

	// anchor point
	//$req .= pack ( "N", 0 );
	binary.Write(buff, binary.BigEndian, int32(0))


	// anchor point
	//$req .= pack ( "N", count($this->_indexweights) );
	//binary.Write(buff, binary.BigEndian, int32(len(indexweights)))


	// per-index weights
	binary.Write(buff, binary.BigEndian, int32(len(s.vars.indexweights)))

	for _,v := range s.vars.indexweights {
		//$req .= pack ( "N", strlen($idx) ) . $idx . pack ( "N", $weight );
		binary.Write(buff, binary.BigEndian, int32(len(v.Idx)))
		buff.Write([]byte(v.Idx))
		binary.Write(buff, binary.BigEndian, int32(v.Weight))

	}

	//$req .= pack ( "N", $this->_maxquerytime );
	binary.Write(buff, binary.BigEndian, int32(s.vars.maxquerytime))


	// per-field weights
	//$req .= pack ( "N", count($this->_fieldweights) );
	binary.Write(buff, binary.BigEndian, int32(len(s.vars.fieldweights)))

	for _,v := range s.vars.fieldweights {
		//$req .= pack ( "N", strlen($field) ) . $field . pack ( "N", $weight );
		binary.Write(buff, binary.BigEndian, int32(len(v.Name)))
		buff.Write([]byte(v.Name))
		binary.Write(buff, binary.BigEndian, int32(v.Weight))
	}

	//$req .= pack ( "N", strlen($comment) ) . $comment;
	binary.Write(buff, binary.BigEndian, int32(len(comment)))
	buff.Write([]byte(comment))

	s.vars.resq = append(s.vars.resq, buff.Bytes())

	return len(s.vars.resq)
}

func (s *Sphinx) getResponse(client_ver string ) ([]byte,error){
	header := make([]byte, 4)

	io.ReadFull(s.Conn, header[:2])
	status :=binary.BigEndian.Uint16(header[:2])


	io.ReadFull(s.Conn, header[:2])
	ver := binary.BigEndian.Uint16(header[:2])


	io.ReadFull(s.Conn, header[:4])
	lens := binary.BigEndian.Uint32(header[:4])


	buff := make([]byte, lens)
	io.ReadFull(s.Conn, buff[:])

	if status == SEARCHD_WARNING {
		wlen := binary.BigEndian.Uint32(buff[:4])
		s.vars.warning = string(buff[4:wlen])
		fmt.Println(s.vars.warning)
		return buff[4+wlen:], nil
	}

	if status == SEARCHD_ERROR {
		return nil, fmt.Errorf("%w: %s", ErrRetryMessage, string(buff[4:]))
	}

	if status == SEARCHD_RETRY{
		return nil, fmt.Errorf("%w: %s", ErrRetryMessage, string(buff[4:]))
	}

	if status != SEARCHD_OK{
		return nil, fmt.Errorf("%w: %s", ErrRetryMessage, "unknown error")
	}

	command_search,_ := hex.DecodeString(client_ver)

	if ver < binary.BigEndian.Uint16(command_search) {
		return nil, fmt.Errorf("%w: %s", ErrRetryMessage, "client_ver error")
	}

	return buff, nil

}