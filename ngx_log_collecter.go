package main

import (
    "bufio"
    "fmt"
    "os"
    "strings"
    //"sort"
    "flag"
    "github.com/astaxie/beego/orm"
    _ "github.com/go-sql-driver/mysql"
    "strconv"
    "time"
)

//写入数据库
type ngx_res struct {
    Id        int64
    Date      time.Time
    Url       string
    Project   string
    Xiaoyu10  int
    Xiaoyu50  int
    Xiaoyu100 int
    Xiaoyu500 int
    Dayu500   int
}
type ngx_ip struct {
    Id      int64
    Date    time.Time
    Project string
    Ip      string
    Times   string
}
type ngx_access struct {
    Id      int64
    Date    time.Time
    Project string
    Code    int64
    Url     string
    Times   int
}
type time_res struct {
    times_10       int
    times_50       int
    times_100      int
    times_500      int
    times_dayu_500 int
}

func Add_access(project string, date string, code string, url string, times int) error {
    o := orm.NewOrm()

    codes, err := strconv.ParseInt(code, 10, 64)
    if err != nil {
        return err
    }
    _, error := o.Raw("INSERT INTO `ngx_access` (`date`, `project`, `code`, `url`, `times`) VALUES (?, ?, ?, ?, ?);", date, project, codes, url, times).Exec()
    return error
}
func Add_ip(project string, date string, ip string, times int) error {
    o := orm.NewOrm()

    _, error := o.Raw("INSERT INTO `ngx_ip` (`date`, `project`, `ip`,`times`) VALUES (?, ?, ?, ?);", date, project, ip, times).Exec()
    return error
}
func Add_res(project string, date string, url string, xiaoyu10 int, xiaoyu50 int, xiaoyu100 int, xiaoyu500 int, dayu500 int) error {
    o := orm.NewOrm()

    _, error := o.Raw("INSERT INTO `ngx_res` (`date`, `project`,`url`,`xiaoyu10`,`xiaoyu50`,`xiaoyu100`,`xiaoyu500`,`dayu500`) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", date, project, url, xiaoyu10, xiaoyu50, xiaoyu100, xiaoyu500, dayu500).Exec()
    return error
}

//初始化数据库
func RegisterDb(uname string, passwd string, ipaddr string, port string, databasename string) {
    orm.RegisterDriver("mysql", orm.DRMySQL)
    orm.RegisterDataBase("default", "mysql", uname+":"+passwd+"@tcp("+ipaddr+":"+port+")/"+databasename+"?charset=utf8", 10)
    orm.RegisterModel(new(ngx_access), new(ngx_ip), new(ngx_res))
}

var hourmap map[string]int = make(map[string]int, 0)
var resmap map[string]time_res = make(map[string]time_res, 0)
var ipmap map[string]int = make(map[string]int, 0)

//读取文件
func read(filename string) {
    fi, err := os.Open(filename)
    if err != nil {
        fmt.Printf("Error: %s\n", err)
        return
    }
    defer fi.Close()

    br := bufio.NewReader(fi)
    for {
        a, err := br.ReadString('\n')
        if err != nil {
            break
        }
        log := string(a)
        //计算每小时访问次数
        split := strings.Split(log, " ")
        ip := split[0]
        date_tmp := strings.Split(split[3], "[")[1]
        date_string := strings.Split(date_tmp, ":")[0]
        //      date_time := time_tihuan(date_string[0],date_string[1])
        url1 := strings.Split(split[5], "?")[0]
        url := strings.Split(url1, "=")[0]
        code := split[1]
        resp, err := strconv.ParseFloat(split[2], 64)
        if err != nil {
            break
        }
        hourmap[date_string+":"+url+":"+code]++
        ipmap[date_string+":"+ip]++
        v, ok := resmap[date_string+":"+url]
        if ok {
            if resp <= 0.01 {
                a := time_res{v.times_10 + 1, v.times_50, v.times_100, v.times_500, v.times_dayu_500}
                resmap[date_string+":"+url] = a
            } else if resp > 0.01 && resp <= 0.05 {
                a := time_res{v.times_10, v.times_50 + 1, v.times_100, v.times_500, v.times_dayu_500}
                resmap[date_string+":"+url] = a
            } else if resp > 0.05 && resp <= 0.1 {
                a := time_res{v.times_10, v.times_50, v.times_100 + 1, v.times_500, v.times_dayu_500}
                resmap[date_string+":"+url] = a
            } else if resp > 0.1 && resp <= 0.5 {
                a := time_res{v.times_10, v.times_50, v.times_100, v.times_500 + 1, v.times_dayu_500}
                resmap[date_string+":"+url] = a
            } else {
                a := time_res{v.times_10, v.times_50, v.times_100, v.times_500, v.times_dayu_500 + 1}
                resmap[date_string+":"+url] = a
            }
        } else {
            if resp <= 0.01 {
                a := time_res{1, 0, 0, 0, 0}
                resmap[date_string+":"+url] = a
            } else if resp > 0.01 && resp <= 0.05 {
                a := time_res{0, 1, 0, 0, 0}
                resmap[date_string+":"+url] = a
            } else if resp > 0.1 && resp <= 0.5 {
                a := time_res{0, 0, 1, 0, 0}
                resmap[date_string+":"+url] = a
            } else if resp > 0.1 && resp <= 0.5 {
                a := time_res{0, 0, 0, 1, 0}
                resmap[date_string+":"+url] = a
            } else {
                a := time_res{0, 0, 0, 0, 1}
                resmap[date_string+":"+url] = a
            }
        }
    }
}

//时间转换函数
func time_tihuan(date_hour string) time.Time {
    //输入时间字符串并拼接
    //time_string := date_hour
    //获取服务器时区
    //loc, _ := time.LoadLocation("Asia/Chongqing")

    //字符串转为时间类型
    theTime, err := time.Parse("2006-01-02T15:04:05 -0700", date_hour)
    if err != nil {
        fmt.Println(err)
    }
    return theTime
}

func init() {
    RegisterDb("username", "password", "ipaddr", "port", "databasename")
}
func main() {
    var filename string
    flag.StringVar(&filename, "filename", "2017-12-35_mobile.log", "nginx access log filename!")
    flag.Parse()
    //read函数 执行后数据统计入map中
    read(filename)
    orm.Debug = true
    orm.RunSyncdb("default", false, true)
    project1 := strings.Split(filename, ".")[0]
    project := strings.Split(project1, "_")[1]
    var hourmap_one map[string]int = make(map[string]int, 0)
    for k, v := range hourmap {
        if v != 1 {
            a := strings.Split(k, ":")
            date := time_tihuan(a[0] + ":00:00 +0800").Format("2006-01-02 15:04:05 -0700")
            Add_access(project, date, a[2], a[1], v)
        } else {
            a := strings.Split(k, ":")
            hourmap_one[a[0]+":oneurl:200"]++
        }
    }
    for k, v := range hourmap_one {
        a := strings.Split(k, ":")
        date := time_tihuan(a[0] + ":00:00 +0800").Format("2006-01-02 15:04:05 -0700")
        Add_access(project, date, a[2], a[1], v)
    }
    for k, v := range ipmap {
        if v > 5 {
            a := strings.Split(k, ":")
            date := time_tihuan(a[0] + ":00:00 +0800").Format("2006-01-02 15:04:05 -0700")
            Add_ip(project, date, a[1], v)
        }
    }
    for k, v := range resmap {

        a := strings.Split(k, ":")
        date := time_tihuan(a[0] + ":00:00 +0800").Format("2006-01-02 15:04:05 -0700")
        Add_res(project, date, a[1], v.times_10, v.times_50, v.times_100, v.times_500, v.times_dayu_500)
    }

}
