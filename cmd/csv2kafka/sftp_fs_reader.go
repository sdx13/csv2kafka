package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

type SftpFilesystemReader struct {
	inputDir     string
	readyDir     string
	waitInterval int

	files          []os.FileInfo
	reader         *GzipReader
	index          int
	privateKeyPath string
	user           string
	password       string
	ip             string
	port           string
	ssh            *ssh.Client
	client         *sftp.Client
}

func getSSHConfig(privateKeyPath, user, password string, hostKeyCheck bool) *ssh.ClientConfig {
	var methods []ssh.AuthMethod
	key, err := ioutil.ReadFile(privateKeyPath)
	if err == nil {
		signer, err := ssh.ParsePrivateKey(key)
		if err == nil {
			methods = append(methods, ssh.PublicKeys(signer))
		}
	}

	methods = append(methods, ssh.Password(password))

	var hostKeyCallback ssh.HostKeyCallback
	if !hostKeyCheck {
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	} else {
		hostKeyCallback, err = knownhosts.New(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
		if err != nil {
			hostKeyCallback = ssh.InsecureIgnoreHostKey()
		}
	}

	return &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: hostKeyCallback,
		Auth:            methods,
	}
}

func getSSHClient(config *ssh.ClientConfig, host, port string) (*ssh.Client, error) {
	return ssh.Dial("tcp", host+":"+port, config)
}

func getSFTPClient(conn *ssh.Client) (*sftp.Client, error) {
	return sftp.NewClient(conn)
}

func (r *SftpFilesystemReader) Read() ([]string, error) {
	if len(r.files) == 0 {
		i := 0
		log.Println("Scanning for files in dir", r.inputDir)
		files, err := r.readDir(r.inputDir)
		if err != nil {
			log.Printf("Failed to read input dir: %v", err)
			return nil, err
		}
		for ; i < len(files); i++ {
			name := filepath.Join(r.inputDir, files[i].Name())
			log.Println("Reading file", name)
			f, err := r.client.Open(name)
			if err != nil {
				log.Println("Failed to open file", err)
				continue
			}
			reader, err := NewGzipReader(f)
			if err == nil {
				r.reader = reader
				r.index = i
				r.files = files
				break
			} else {
				log.Printf("Could not open file for reading: %v", err)
			}
		}
		if i == len(files) {
			log.Println("No input file found")
			log.Printf("Waiting for %v seconds", r.waitInterval)
			time.Sleep(time.Duration(r.waitInterval) * time.Second)
			return r.Read()
		}
	}
	record, err := r.reader.Read()
	if record == nil {
		err := r.close()
		currentName := r.files[r.index].Name()
		if err != nil {
			fmt.Printf("Error closing file %v: %v", currentName, err)
		} else {
			log.Println("Closed file", currentName)
		}
		r.files = r.files[1:]
		r.postProcess(currentName)
		return r.Read()
	}
	return record, err
}

func (r *SftpFilesystemReader) close() error {
	// XXX/PDP audit this
	//return r.reader.Close()
	return nil
}

func (r *SftpFilesystemReader) postProcess(name string) {
	from := filepath.Join(r.inputDir, name)
	to := filepath.Join(r.readyDir, name)
	err := r.client.Rename(from, to)
	if err != nil {
		fmt.Printf("Failed to rename %v to %v: %v", from, to, err)
	} else {
		log.Println("Processed", name)
	}
}

func (r *SftpFilesystemReader) sftpInit() error {
	// XXX/PDP take this from config
	hostKeyCheck := false
	privateKeyPath := r.privateKeyPath
	user := r.user
	pass := r.password
	ip := r.ip
	port := r.port
	sshConfig := getSSHConfig(privateKeyPath, user, pass, hostKeyCheck)
	sshClient, err := getSSHClient(sshConfig, ip, port)
	if err != nil {
		return err
	}
	sftpClient, err := getSFTPClient(sshClient)
	if err != nil {
		return err
	}
	r.ssh = sshClient
	r.client = sftpClient
	return nil
}

func (r *SftpFilesystemReader) readDir(path string) ([]os.FileInfo, error) {
	if r.client == nil {
		err := r.sftpInit()
		if err != nil {
			return nil, err
		}
	}
	/*
		f, err := r.client.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
	*/
	filesInfo, err := r.client.ReadDir(path)
	if err != nil {
		return nil, err
	}
	return filesInfo, nil
}
