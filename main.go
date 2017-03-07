/*
Read objects from two sites and record success over time
 */
package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type Obj struct {
	bucket string
	key    string
}

func main() {
	endpoint1 := os.Args[1]
	endpoint2 := os.Args[2]
	fileName1 := os.Args[3]
	fileName2 := os.Args[4]
	outFileName := os.Args[5]

	fmt.Printf("Hello World!\n")
	sess1, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: &endpoint1,
		Credentials: credentials.NewSharedCredentials("", "default"),
	})

	if err != nil {
		panic(fmt.Sprintf("Could not connect to endpoint1: %s", err))
	}
	sess2, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: &endpoint2,
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

	outFile, err := os.OpenFile(outFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0444)
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
	buffer := make([]byte, 128*1024)
	for _, o := range objs {
		out, err := con.GetObject(&s3.GetObjectInput{Bucket: &o.bucket, Key: &o.key})
		if err != nil {
			log.Printf("Could not get %s/%s: %s", o.bucket, o.key, err)
			continue
		}
		// Read data and discard
		b := out.Body
		for {
			_, err := b.Read(buffer)
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
		}
		b.Close()
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
