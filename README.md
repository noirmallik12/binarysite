import asyncio
import json
import os
import random
import time
import sqlite3
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()

# Destination tracking route for platform capital flows
PAYMENT_ROUTING_CONFIG = {
    "destinationNumber": "0142571572",
    "providerChannel": "DEFAULT_GATEWAY"
}

DB_PATH = "trading.db"

# 1. DATABASE CONFIGURATION & INITIALIZATION
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # A. Users Portfolio Balance Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            balance REAL NOT NULL DEFAULT 10000.00,
            frozen_balance REAL NOT NULL DEFAULT 0.00
        )
    """)
    
    # B. Asset Shares Position Tracking Inventory
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shares (
            asset TEXT PRIMARY KEY,
            quantity INTEGER NOT NULL DEFAULT 0
        )
    """)
    
    # C. Pending Limit Order Book Registry
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_book (
            id INTEGER PRIMARY KEY,
            asset TEXT NOT NULL,
            action TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            target_price REAL NOT NULL,
            escrow_value REAL NOT NULL DEFAULT 0.00
        )
    """)
    
    # Seed default baseline parameters safely
    cursor.execute("INSERT OR IGNORE INTO portfolio (id, balance, frozen_balance) VALUES (1, 10000.00, 0.00)")
    for ticker in ['BTC', 'ETH', 'AAPL']:
        cursor.execute("INSERT OR IGNORE INTO shares (asset, quantity) VALUES (?, 0)", (ticker,))
        
    conn.commit()
    conn.close()

init_db()

def get_portfolio_state():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    portfolio_row = cursor.execute("SELECT balance, frozen_balance FROM portfolio WHERE id = 1").fetchone()
    share_rows = cursor.execute("SELECT asset, quantity FROM shares").fetchall()
    conn.close()
    
    shares = {row["asset"]: row["quantity"] for row in share_rows}
    return {
        "balance": portfolio_row["balance"],
        "frozen_balance": portfolio_row["frozen_balance"],
        "shares": shares
    }

# 2. LIVE PRICE ENGINE SIMULATION SETUP
current_prices = {"BTC": 62000.00, "ETH": 3200.00, "AAPL": 175.00}
volatility = {"BTC": 15.00, "ETH": 2.50, "AAPL": 0.20}

# 3. REAL-TIME MATCH ENGINE CORE
async def check_and_execute_limit_orders(websocket: WebSocket):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    orders = cursor.execute("SELECT * FROM order_book").fetchall()
    
    for order in orders:
        market_price = current_prices[order["asset"]]
        actual_market_value = market_price * order["quantity"]
        state = get_portfolio_state()
        
        if order["action"] == "BUY" and market_price <= order["target_price"]:
            refund_value = order["escrow_value"] - actual_market_value
            
            cursor.execute("UPDATE portfolio SET frozen_balance = frozen_balance - ?, balance = balance + ? WHERE id = 1", (order["escrow_value"], refund_value))
            cursor.execute("INSERT INTO shares (asset, quantity) VALUES(?, ?) ON CONFLICT(asset) DO UPDATE SET quantity = quantity + ?", (order["asset"], order["quantity"], order["quantity"]))
            cursor.execute("DELETE FROM order_book WHERE id = ?", (order["id"],))
            conn.commit()
            
            await websocket.send_json({"type": "ORDER_FILLED", "message": f"Limit BUY executed for {order['quantity']} {order['asset']} at ${market_price}."})
            
        elif order["action"] == "SELL" and market_price >= order["target_price"]:
            available_shares = state["shares"].get(order["asset"], 0)
            if available_shares >= order["quantity"]:
                cursor.execute("UPDATE portfolio SET balance = balance + ? WHERE id = 1", (actual_market_value,))
                cursor.execute("UPDATE shares SET quantity = quantity - ? WHERE asset = ?", (order["quantity"], order["asset"]))
                cursor.execute("DELETE FROM order_book WHERE id = ?", (order["id"],))
                conn.commit()
                
                print(f"[ROUTING] Asset liquidation transfer handled via clearing destination: {PAYMENT_ROUTING_CONFIG['destinationNumber']}")
                await websocket.send_json({"type": "ORDER_FILLED", "message": f"Limit SELL executed for {order['quantity']} {order['asset']} at ${market_price}"})
                
    conn.close()

# 4. WEBSOCKET PIPELINE STREAMING
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("Active trader connected to Python multi-asset streams.")
    try:
        while True:
            # Advance price updates simulation
            for ticker in ["BTC", "ETH", "AAPL"]:
                movement = (random.random() - 0.5) * 2 * volatility[ticker]
                current_prices[ticker] = round(current_prices[ticker] + movement, 2)
                
            await check_and_execute_limit_orders(websocket)
            current_portfolio = get_portfolio_state()
            
            await websocket.send_json({
                "type": "PRICE_UPDATE",
                "BTC": current_prices["BTC"],
                "ETH": current_prices["ETH"],
                "AAPL": current_prices["AAPL"],
                "portfolio": current_portfolio
            })
            await asyncio.sleep(1) # Broadcast feed updates every 1 second
    except WebSocketDisconnect:
        print("Trader disconnected from streaming sockets.")

# 5. REST API SCHEMAS & ROUTES
class TradeRequest(BaseModel):
    action: str
    quantity: int
    asset: str

class LimitOrderRequest(BaseModel):
    action: str
    quantity: int
    asset: str
    targetPrice: float

@app.post("/api/trade")
async def handle_market_trade(req: TradeRequest):
    if req.asset not in current_prices:
        raise HTTPException(status_code=400, detail="Unsupported asset ticker")
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Invalid volume value")
        
    price_at_execution = current_prices[req.asset]
    total_transaction_value = price_at_execution * req.quantity
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    state = get_portfolio_state()
    
    try:
        if req.action == "BUY":
            if state["balance"] < total_transaction_value:
                raise HTTPException(status_code=400, detail="Insufficient funds")
                
            cursor.execute("UPDATE portfolio SET balance = balance - ? WHERE id = 1", (total_transaction_value,))
            cursor.execute("INSERT INTO shares (asset, quantity) VALUES(?, ?) ON CONFLICT(asset) DO UPDATE SET quantity = quantity + ?", (req.asset, req.quantity, req.quantity))
        elif req.action == "SELL":
            current_shares_owned = state["shares"].get(req.asset, 0)
            if current_shares_owned < req.quantity:
                raise HTTPException(status_code=400, detail="Insufficient inventory positions")
                
            cursor.execute("UPDATE portfolio SET balance = balance + ? WHERE id = 1", (total_transaction_value,))
            cursor.execute("UPDATE shares SET quantity = quantity - ? WHERE asset = ?", (req.quantity, req.asset))
            print(f"[LIQUIDATION] Outbound payout mapped for destination reference validation: {PAYMENT_ROUTING_CONFIG['destinationNumber']}")
        else:
            raise HTTPException(status_code=400, detail="Invalid action value")
            
        conn.commit()
        updated_state = get_portfolio_state()
        return {"success": True, "portfolio": updated_state, "executionPrice": price_at_execution}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/limit-order")
async def handle_limit_order(req: LimitOrderRequest):
    if req.asset not in current_prices:
        raise HTTPException(status_code=400, detail="Unsupported asset ticker")
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="Invalid quantity")
    if req.targetPrice <= 0:
        raise HTTPException(status_code=400, detail="Invalid target price")
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    state = get_portfolio_state()
    escrow_value = 0.0
    
    try:
        if req.action == "BUY":
            escrow_value = float(req.targetPrice) * int(req.quantity)
            if state["balance"] < escrow_value:
                raise HTTPException(status_code=400, detail=f"Insufficient liquid balance. Need ${escrow_value:.2f}")
                
            cursor.execute("UPDATE portfolio SET balance = balance - ?, frozen_balance = frozen_balance + ? WHERE id = 1", (escrow_value, escrowValue))
            
        cursor.execute("INSERT INTO order_book (id, asset, action, quantity, target_price, escrow_value) VALUES (?, ?, ?, ?, ?, ?)",
                       (int(time.time() * 1000), req.asset, req.action, int(req.quantity), float(req.targetPrice), escrow_value))
        conn.commit()
        return {"success": True, "message": f"Limit {req.action} order registered for {req.quantity} {req.asset} at ${req.targetPrice}."}
    finally:
        conn.close()

@app.get("/api/limit-orders")
async def get_limit_orders():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    rows = cursor.execute("SELECT id, asset, action, quantity, target_price AS targetPrice FROM order_book").fetchall()
    conn.close()
    return {"success": True, "orders": [dict(row) for row in rows]}

@app.delete("/api/limit-order/{order_id}")

