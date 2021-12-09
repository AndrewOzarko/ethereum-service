package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/andrewozarko/ethereum-service/uniswap"
	"github.com/andrewozarko/ethereum-service/uniswap/uniswap_pairs"
	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

const factoryAddress = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

var client = &ethclient.Client{}

var N = int64(runtime.NumCPU()) * 2

var db *bolt.DB

type Pair struct {
	ID               int64
	Address          string
	Pair0Address     string
	Pair1Address     string
	Pair0Name        string
	Pair0Symbol      string
	Pair0Decimals    uint8
	Pair0TotalSupply int
	Pair1Name        string
	Pair1Symbol      string
	Pair1Decimals    uint8
	Pair1TotalSupply int
	Pair0            uniswap_pairs.UniswapPairs
	Pair1            uniswap_pairs.UniswapPairs
}

type WorkersPool struct {
	CntOfJobs  int64
	InputData  chan int64
	OutputData chan Pair
}

var globalLoger = &zap.SugaredLogger{}

func main() {
	var err error

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	globalLoger = logger.Sugar()

	if err := godotenv.Load(); err != nil {
		globalLoger.Fatal("No .env")
	}

	client, err = ethclient.Dial("wss://main-light.eth.linkpool.io/ws")
	if err != nil {
		globalLoger.Warn(err)
	}

	defer client.Close()

	address := common.HexToAddress(factoryAddress)
	us, err := uniswap.NewUniswap(address, client)

	if err != nil {
		globalLoger.Warn(err)
	}

	uscont, err := us.AllPairsLength(&bind.CallOpts{})

	if err != nil {
		globalLoger.Warn(err)
	}
	log.Printf("found %d, pairs:", uscont)

	db, err = bolt.Open("uniswap.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		globalLoger.Warn(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(int(uscont.Uint64()))

	wp := NewWorkesPool()
	go func() {
		for wpcount := int64(1); wpcount < wp.CntOfJobs; wpcount++ {
			go uniswapWorker(wp, *us, &wg)
		}
		go uniswapWorkerInitter(wp, uscont)
		wg.Wait()
		close(wp.OutputData)
	}()

	var paddress = []common.Address{}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("pairs"))
		b.ForEach(func(k, _ []byte) error {
			paddress = append(paddress, common.HexToAddress(string(k)))
			return nil
		})
		return nil
	})
	if err != nil {
		globalLoger.Warn(err)
	}

	fmt.Printf("looking for %d pairs", len(paddress))

	time.Sleep(2 * time.Second)

	query := ethereum.FilterQuery{
		Addresses: paddress,
	}

	logs := make(chan types.Log)
	sub, err := client.SubscribeFilterLogs(context.Background(), query, logs)
	if err != nil {
		globalLoger.Warn(err)
	}

	for {
		select {
		case err := <-sub.Err():
			globalLoger.Warn(err)
		case vLog := <-logs:
			a := vLog.Address.String()

			db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("pairs"))
				v := b.Get([]byte(a))
				if v != nil {
					var pair = Pair{}
					json.Unmarshal([]byte(v), &pair)
					fmt.Printf("Change in liquidity for %s (%s) - %s (%s) pair \n", pair.Pair0Name, pair.Pair0Symbol, pair.Pair1Name, pair.Pair1Symbol)
					time.Sleep(1 * time.Second)
				}
				return nil
			})
		}
	}
}

func NewWorkesPool() *WorkersPool {
	return &WorkersPool{
		CntOfJobs:  int64(N),
		InputData:  make(chan int64, N),
		OutputData: make(chan Pair, N),
	}
}

func uniswapWorkerInitter(wp *WorkersPool, uscont *big.Int) {

	go func() {
		for i := int64(1); i <= uscont.Int64(); i++ {
			wp.InputData <- i
		}
		close(wp.InputData)
	}()

	for {
		select {

		case v, status := <-wp.OutputData:

			if !status {
				return
			}

			err := db.Batch(func(tx *bolt.Tx) error {

				b, err := tx.CreateBucketIfNotExists([]byte("pairs"))
				if err != nil {
					log.Println(err)
				}

				buf, err := json.Marshal(v)
				if err != nil {
					return err
				}

				return b.Put([]byte(v.Address), buf)

			})

			if err != nil {
				globalLoger.Warn(err)
			}

			showInConsole, exists := os.LookupEnv("SHOW_IN_CONSOLE_PAIR_DATA")

			if exists && showInConsole == "true" {
				fmt.Printf("%d, %s, %s(%s) - %s(%s), %s, %s \n", v.ID, v.Address, v.Pair0Name, v.Pair0Symbol, v.Pair1Name, v.Pair1Symbol, v.Pair0Address, v.Pair1Address)
			}
		}
	}
}

func uniswapWorker(wp *WorkersPool, us uniswap.Uniswap, wg *sync.WaitGroup) {
	var err error
	var addr common.Address
	var p *uniswap_pairs.UniswapPairs

	for j := range wp.InputData {
		addr, err = us.AllPairs(&bind.CallOpts{}, big.NewInt(j))
		if err != nil {
			globalLoger.Warn(err)
		}
		p, err = uniswap_pairs.NewUniswapPairs(addr, client)
		if err != nil {
			globalLoger.Warn(err)
		}
		t1, err := p.Token0(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		t2, err := p.Token1(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p0, err := uniswap_pairs.NewUniswapPairs(t1, client)
		if err != nil {
			globalLoger.Warn(err)
		}
		p1, err := uniswap_pairs.NewUniswapPairs(t2, client)
		if err != nil {
			globalLoger.Warn(err)
		}

		p0Name, err := p0.Name(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p0Symbol, err := p0.Symbol(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p0Decimals, err := p0.Decimals(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p0TotalSupply, err := p0.Decimals(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p1Name, err := p1.Name(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p1Symbol, err := p1.Symbol(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p1Decimals, err := p1.Decimals(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		p1TotalSupply, err := p1.Decimals(&bind.CallOpts{})
		if err != nil {
			globalLoger.Warn(err)
		}
		wp.OutputData <- Pair{
			ID:               j,
			Address:          addr.String(),
			Pair0Address:     t1.String(),
			Pair1Address:     t1.String(),
			Pair0Name:        p0Name,
			Pair0Symbol:      p0Symbol,
			Pair0Decimals:    p0Decimals,
			Pair0TotalSupply: int(p0TotalSupply),
			Pair1Name:        p1Name,
			Pair1Symbol:      p1Symbol,
			Pair1Decimals:    p1Decimals,
			Pair1TotalSupply: int(p1TotalSupply),
			Pair0:            *p0,
			Pair1:            *p1,
		}
		wg.Done()
	}
}
