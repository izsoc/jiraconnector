package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type jiraProject struct {
	Key string `json:"key"`
}

type jiraIssueType struct {
	Id string `json:"id"`
}

type jiraFields struct {
	Project       jiraProject   `json:"project"`
	Issuetype     jiraIssueType `json:"issuetype"`
	Summary       string        `json:"summary"`
	Description   string        `json:"description"`
	SrcIP         string        `json:"customfield_10225"`
	DstIP         string        `json:"customfield_10226"`
	Organizations [1]int        `json:"customfield_10002"`
	Priority      jiraPriority  `json:"priority"`
	Srcname       string        `json:"customfield_10230"`
	Dstname       string        `json:"customfield_10229"`
}

type jiraMsg struct {
	Fields jiraFields `json:"fields"`
}

type jiraPriority struct {
	Priority string `json:"id"`
}
type kafkaMsg struct {
	Logsource   string `json:"logsource"`
	Logtype     string `json:"class"`
	Timestamp   string `json:"@timestamp"`
	ProjID      string `json:"type"`
	OrgID       string `json:"orgid"`
	Message     string `json:"message"`
	Summary     string `json:"summary"`
	Description string `json:"desc"`
}

var fields = []string{"logsource", "class", "@timestamp", "type", "orgid", "message", "summary", "desc", "srcip", "dstip"}

type server struct {
}

var (
	kafkaURL      = flag.String("kafka-broker", "127.0.0.1:9092", "Kafka broker URL list")
	intopic       = flag.String("kafka-in-topic", "notopic", "Kafka topic to read from")
	jiraURL       = flag.String("jira-url", "127.0.0.1", "Jira instance")
	groupID       = flag.String("kafka-group", "nogroup", "Kafka group")
	statPort      = flag.String("stat-port", "1234", "Port to bind statistic endpoint")
	jiraProjectID = flag.String("jira-project", "TI", "Jira project name")
	user          = flag.String("user", "gw", "user name for Jira")
	pass          = flag.String("pass", "1234", "Password for Jira")
	alert         jiraMsg
	logger        *log.Logger
	reader        *kafka.Reader
)

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	str, _ := json.Marshal(reader.Stats())
	w.Write([]byte(str))
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     3 * time.Second,
		StartOffset: kafka.LastOffset,
		ErrorLogger: logger,
	})
}

func convertKafkaJira(message map[string]interface{}) jiraMsg {
	var msg jiraMsg
	msg.Fields.Organizations[0] = 8
	msg.Fields.Issuetype.Id = "10007"
	msg.Fields.Priority.Priority = "4"
	msg.Fields.Project.Key = *jiraProjectID

	// "logsource", "class", "@timestamp", "type", "orgid", "message", "summary", "desc", "srcip", "dstip"

	for _, f := range fields {

		s, found := message[f]

		if found {

			switch f {
			case "logsource":
				//msg.Logsource = s.(string)

			case "class":
				//msg.Logtype = s.(string)

			case "type":
				msg.Fields.Project.Key = s.(string)

			case "orgid":
				id, err := strconv.Atoi(s.(string))
				if err == nil {
					msg.Fields.Organizations[0] = id
				}

			case "message":
				msg.Fields.Description += s.(string)

			case "summary":
				msg.Fields.Summary = s.(string)

			case "desc":
				msg.Fields.Description = s.(string)
			case "srcip":
				msg.Fields.SrcIP = s.(string)
				namesSrc, err := net.LookupAddr(s.(string))

				if err != nil {
					msg.Fields.Srcname = ""
				} else {
					msg.Fields.Srcname = strings.Join(namesSrc, ";")
				}
			case "dstip":
				msg.Fields.DstIP = s.(string)
				namesDst, err := net.LookupAddr(s.(string))
				if err != nil {
					msg.Fields.Dstname = ""
				} else {
					msg.Fields.Dstname = strings.Join(namesDst, " ")
				}

			}
		}

	}

	return msg
}

func init() {
	flag.Parse()
	logger = log.New(os.Stdout, "kafkajira:", log.Ldate|log.Ltime)
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func sendJiraAlert(message map[string]interface{}) {

	jsonValue, _ := json.Marshal(convertKafkaJira(message))

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{
		Transport: tr,
	}

	req, err := http.NewRequest("POST", *jiraURL+"/rest/api/2/issue", bytes.NewBuffer(jsonValue))

	req.Header.Add("Authorization", "Basic "+basicAuth(*user, *pass))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		logger.Printf("The HTTP request failed with error %s\n", err)
	}

	resp.Body.Close()

}

//src ip - 8 hihgest byte, dst ip - 8 lowest byte map to last seen

func main() {

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	reader = getKafkaReader(*kafkaURL, *intopic, *groupID)

	defer func() {
		reader.Close()
		close(sigs)
	}()

	go func() {
		s := &server{}
		http.Handle("/metrics", s)
		logger.Fatal(http.ListenAndServe(":"+*statPort, nil))
	}()

	logger.Println("start consuming ... !!")

	var f interface{}

	start := time.Now()

loop:
	for {

		select {
		case sig := <-sigs:
			logger.Println(sig)
			break loop
		default:
			m, err := reader.ReadMessage(context.Background())

			if err != nil {
				logger.Println(err)
				break
			}

			err = json.Unmarshal([]byte(m.Value), &f)

			if err != nil {
				logger.Println(err)
				break
			}

			if err != nil {
				logger.Print(err)
				break
			}

			msg := f.(map[string]interface{})

			sendJiraAlert(msg)
		}
	}

	logger.Println("Terminating")
	elapsed := time.Since(start)
	logger.Printf("Message processed %d in %s\n", reader.Stats().Messages, elapsed)

}
