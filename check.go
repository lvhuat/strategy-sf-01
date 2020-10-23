package main

import (
	"time"

	"github.com/google/uuid"

	"github.com/sirupsen/logrus"
)

func check() bool {
	var spot, future *MarketItem
	since := time.Now()
	wg := WaitGroupExecutor{}
	wg.Add(2)
	wg.Run(func() error {
		resp, err := client.getMarket(spotName)
		if err != nil {
			return err
		}
		spot = resp
		return nil
	})

	wg.Run(func() error {
		resp, err := client.getMarket(futureName)
		if err != nil {
			return err
		}
		future = resp
		return nil
	})

	wg.Wait()
	// 高延迟行情不处理
	takeTime := time.Now().Sub(since)
	if takeTime > time.Millisecond*3000 {
		return false
	}

	open := (future.Bid - spot.Ask) / spot.Ask
	close := (future.Ask - spot.Bid) / spot.Bid

	changed := false
	for index, grid := range grids {
		// 触发开
		if open >= grid.OpenAt && grid.OpenChance > 0 {
			qty := grid.PlaceQty
			pair := NewHedgePair(grid, true, qty)
			grid.OpenPairs[uuid.New().String()] = pair
			grid.OpenChance--
			persistGrids()

			place(pair.Spot.ClientId, spotName, "buy", 0, "market", qty, false, false)
			place(pair.Spot.ClientId, futureName, "sell", 0, "market", qty, false, false)
			orderMap.add(pair.Future)
			orderMap.add(pair.Spot)

			changed = true
		}

		// 触发平
		if close <= grid.CloseAt && grid.CloseChance > 0 {
			qty := grid.PlaceQty
			pair := NewHedgePair(grid, false, qty)
			grid.CloseChance--
			grid.ClosePairs[uuid.New().String()] = pair
			persistGrids()

			place(pair.Spot.ClientId, spotName, "sell", 0, "market", qty, false, false)
			place(pair.Spot.ClientId, futureName, "buy", 0, "market", qty, false, false)
			orderMap.add(pair.Future)
			orderMap.add(pair.Spot)

			changed = true
		}

		if changed {
			log.WithFields(logrus.Fields{
				"openChance":  grid.OpenChance,
				"closeChance": grid.CloseChance,
			}).Infoln("Grid triggered", index)
			break
		}
	}

	return changed
}

func onOrderChange(order *Order) {
	gridOrder, found := orderMap.get(order.ClientID)
	if !found {
		return
	}

	delta := order.FilledSize - gridOrder.EQty
	closed := order.Status == "closed"
	pair := gridOrder.Pair // 订单归属网格

	if gridOrder.Id == 0 {
		gridOrder.Id = order.ID
	}
	gridOrder.UpdateTime = time.Now()

	// 订单未处理成交部分
	if delta > 0.0 {
		gridOrder.EQty = order.FilledSize
		switch {
		case order.Market == spotName:
			pair.SpotQty += delta
		case order.Market == futureName:
			pair.FutureQty += delta
		}
	}

	// 订单关闭处理未成交部分
	if closed {
		switch {
		case order.Market == spotName:
			gridOrder.Closed = true
		case order.Market == futureName:
			gridOrder.Closed = true
		}

		// 从全局订单表中移除订单
		orderMap.remove(order.ClientID)
	}
}

func onRejectOrder(clientId string) {
	logrus.Infoln("RejectOrder", clientId)
	gridOrder, found := orderMap.get(clientId)
	if !found {
		return
	}
	pair := gridOrder.Pair // 订单归属网格

	switch {
	case pair.Spot.ClientId == clientId:
		pair.Spot.Closed = true
		pair.Spot.FinishAt = time.Now()
	case pair.Future.ClientId == clientId:
		pair.Future.Closed = true
		pair.Future.FinishAt = time.Now()
	}

	orderMap.remove(clientId)
}

var RejectOrder func(clientId string)

func checkPairs() {
	for _, grid := range grids {
		for uid, pair := range grid.OpenPairs {
			// 订单未结束，继续等待
			if !pair.Spot.Closed || !pair.Future.Closed {
				continue
			}

			// 重试超过5次，则放弃
			if pair.RetryPlace > 5 {
				continue
			}

			// 两边一起失败，则放弃
			if pair.SpotQty == 0 && pair.FutureQty == 0 {
				delete(grid.OpenPairs, uid)
			}

			pair.RetryPlace++
			// 现货补单
			if pair.SpotQty != pair.TargetQty {
				pair.Spot = &GridOrder{
					ClientId: uuid.New().String(),
					Qty:      pair.TargetQty - pair.SpotQty,
					CreateAt: time.Now(),
					Pair:     pair,
				}
				persistGrids()
				orderMap.add(pair.Spot)
				place(pair.Spot.ClientId, spotName, "buy", 0, "market", pair.Spot.Qty, false, false)
			}

			// 期货补单
			if pair.FutureQty != pair.TargetQty {
				pair.Future = &GridOrder{
					ClientId: uuid.New().String(),
					Qty:      pair.TargetQty - pair.FutureQty,
					CreateAt: time.Now(),
					Pair:     pair,
				}
				persistGrids()
				orderMap.add(pair.Future)
				place(pair.Future.ClientId, futureName, "sell", 0, "market", pair.Future.Qty, false, false)
			}
		}

		for uid, pair := range grid.ClosePairs {
			// 订单未结束，继续等待
			if !pair.Spot.Closed || !pair.Future.Closed {
				continue
			}

			// 两边一起失败，则放弃
			if pair.SpotQty == 0 && pair.FutureQty == 0 {
				delete(grid.ClosePairs, uid)
			}

			// 重试超过5次，则放弃
			if pair.RetryPlace > 5 {
				continue
			}
			pair.RetryPlace++

			// 现货补单
			if pair.SpotQty != pair.TargetQty { // pair.Spot = nil
				pair.Spot = &GridOrder{
					ClientId: uuid.New().String(),
					Qty:      pair.TargetQty - pair.SpotQty,
					CreateAt: time.Now(),
					Pair:     pair,
				}
				orderMap.add(pair.Spot)
				persistGrids()
				place(pair.Spot.ClientId, spotName, "sell", 0, "market", pair.Spot.Qty, false, false)
			}

			// 期货补单
			if pair.FutureQty != pair.TargetQty {
				pair.Future = &GridOrder{
					ClientId: uuid.New().String(),
					Qty:      pair.TargetQty - pair.FutureQty,
					CreateAt: time.Now(),
					Pair:     pair,
				}
				persistGrids()
				orderMap.add(pair.Future)
				place(pair.Future.ClientId, futureName, "buy", 0, "market", pair.Future.Qty, false, false)
			}
		}
	}
}
