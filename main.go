/*
Read objects from two sites and record success over time
 */
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

var (
	forcePathStyle bool = true
	profile string = "default"
)

const (
	numArgs = 5
)

type Obj struct {
	bucket string
	key    string
}

func init(){
	flag.BoolVar(&forcePathStyle, "forcepathstyle", forcePathStyle, "disable S3 path style")
	flag.StringVar(&profile, "profile", profile, "AWS config file profile")
	flag.Parse()
	if flag.NArg() < numArgs {
		fmt.Fprintln(os.Stderr, "invalid number of args")
	}
}

func main() {
	endpoint1 := flag.Arg(0)
	endpoint2 := flag.Arg(1)
	fileName1 := flag.Arg(2)
	fileName2 := flag.Arg(3)
	outFileName := flag.Arg(4)

	retries := 0

	fmt.Printf("Hello World!\n")
	sess1, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: &endpoint1,
		S3ForcePathStyle: &forcePathStyle,
		MaxRetries: &retries,
		Credentials: credentials.NewSharedCredentials("", profile),
	})

	if err != nil {
		panic(fmt.Sprintf("Could not connect to endpoint1: %s", err))
	}
	sess2, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: &endpoint2,
		MaxRetries: &retries,
		S3ForcePathStyle: &forcePathStyle,
		Credentials: credentials.NewSharedCredentials("", profile),
	})

	if err != nil {
		panic(fmt.Sprintf("Could not connect to endpoint1: %s", err))
	}

	site1 := s3.New(sess1)
	site2 := s3.New(sess2)

	file1, err := os.Open(fileName1)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s: %s", fileName1, err))
	}
	defer file1.Close()
	site1Objs := readObjs(file1)

	file2, err := os.Open(fileName2)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s: %s", fileName2, err))
	}
	defer file2.Close()
	site2Objs := readObjs(file2)

	outFile, err := os.OpenFile(outFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s: %s", outFileName, err))
	}
	defer outFile.Close()

	outFile.WriteString("Timestamp, s1o1, s1o2, s2o1, s2o2\n")

	// Loop until ^C
	round := 1
	for {
		log.Printf("Round %d, Fight!\n", round)
		s1o1 := make(chan int)
		s1o2 := make(chan int)
		s2o1 := make(chan int)
		s2o2 := make(chan int)

		go getObjects(site1, site1Objs, s1o1)
		go getObjects(site1, site2Objs, s1o2)
		go getObjects(site2, site1Objs, s2o1)
		go getObjects(site2, site2Objs, s2o2)

		now := time.Now()
		outFile.WriteString(fmt.Sprintf("%s,%d,%d,%d,%d\n", now.Format(time.Stamp), <-s1o1, <-s1o2, <-s2o1, <-s2o2))
		outFile.Sync()

		round++
	}

}

func getObjects(con *s3.S3, objs []Obj, c chan int) {
	good := 0
	for _, o := range objs {
		out, err := con.GetObject(&s3.GetObjectInput{Bucket: &o.bucket, Key: &o.key})
		if err != nil {
			log.Printf("Could not get %s/%s: %s", o.bucket, o.key, err)
			continue
		}
		// Read data and discard
		if _, err := ioutil.ReadAll(out.Body); err != nil {
						fmt.Fprintln(os.Stderr, "error reading body:", err)
		}
		good++
	}

	c <- good
}

func readObjs(f *os.File) []Obj {
	s := make([]Obj, 0, 100)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "/", 2)
		log.Printf("bucket: %s key: %s", parts[0], parts[1])
		s = append(s, Obj{bucket: parts[0], key: parts[1]})
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return s
}
