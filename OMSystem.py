from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
import uvicorn
import os
import sqlite3
import json

# ==========================================
# 1. BANCO DE DADOS LOCAL E MIGRAÇÃO AUTOMÁTICA
# ==========================================
SQLALCHEMY_DATABASE_URL = "sqlite:///./orion_oms.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class OrderModel(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(String, index=True)
    ticker = Column(String, index=True)
    option_name = Column(String, index=True)
    price = Column(Float, default=0.0)
    real_price = Column(Float, default=0.0)
    current_price = Column(Float, default=0.0)
    option_type = Column(String) # CALL ou PUT
    strike = Column(Float, default=0.0)
    spot_price = Column(Float, default=0.0)
    direction = Column(String, default="BUY")
    action = Column(String, default="EXECUTION")
    magic = Column(Integer, default=123456)
    lots = Column(Integer, default=100)
    sl_perc = Column(Float, default=0.0)
    tp_perc = Column(Float, default=0.0)
    analysis = Column(String, nullable=True)
    order_type = Column(String, default="A MERCADO")
    status = Column(String, default="FILLED") # PENDING, FILLED, REJECTED, CLOSED, CLOSE_REQUESTED
    error_message = Column(String, nullable=True)
    invested_capital = Column(Float, default=0.0)
    fees = Column(Float, default=0.0)
    pnl_brl = Column(Float, default=0.0)
    pnl_perc = Column(Float, default=0.0)
    expiration = Column(String, nullable=True)
    account_id = Column(String, default=os.getenv("FENIX_ACCOUNT", "70960268"))
    created_at = Column(DateTime, default=datetime.now)
    closed_at = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

def check_and_migrate_db():
    db_path = "orion_oms.db"
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        colunas_necessarias = {
            "error_message": "TEXT",
            "invested_capital": "FLOAT DEFAULT 0.0",
            "fees": "FLOAT DEFAULT 0.0",
            "current_price": "FLOAT DEFAULT 0.0",
            "pnl_brl": "FLOAT DEFAULT 0.0",
            "pnl_perc": "FLOAT DEFAULT 0.0",
            "expiration": "TEXT",
            "account_id": "TEXT DEFAULT '70960268'",
            "closed_at": "TIMESTAMP"
        }
        for col, tipo in colunas_necessarias.items():
            try:
                cursor.execute(f"SELECT {col} FROM orders LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute(f"ALTER TABLE orders ADD COLUMN {col} {tipo}")
                print(f"[MIGRAÇÃO] Coluna '{col}' adicionada com sucesso!")
        conn.commit()
        conn.close()

check_and_migrate_db()

# ==========================================
# 2. INICIALIZAÇÃO DA API FASTAPI
# ==========================================
app = FastAPI(
    title="FENIX ENTERPRISE OMS - ProfitPro Engine",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

START_TIME = datetime.now()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class OrderPayload(BaseModel):
    timestamp: Optional[str] = None
    ticker: str
    option: str
    price: float
    ask: Optional[float] = 0.0
    real_price: Optional[float] = 0.0
    current_price: Optional[float] = 0.0
    option_type: Optional[str] = "CALL"
    strike: Optional[float] = 0.0
    spot_price: Optional[float] = 0.0
    direction: Optional[str] = "BUY"
    action: Optional[str] = "EXECUTION"
    magic: Optional[int] = 123456
    lots: Optional[int] = 100
    sl_perc: Optional[float] = 20.0
    tp_perc: Optional[float] = 100.0
    analysis: Optional[str] = ""
    order_type: Optional[str] = "A MERCADO"
    status: Optional[str] = "FILLED"
    error_message: Optional[str] = None
    invested_capital: Optional[float] = 0.0
    fees: Optional[float] = 0.0
    expiration: Optional[str] = ""
    account_id: Optional[str] = None

class MarketPriceUpdateItem(BaseModel):
    option: str
    current_price: float

class MarketUpdatePayload(BaseModel):
    quotes: List[MarketPriceUpdateItem]

# ==========================================
# 3. ENDPOINTS DE RECEPÇÃO E GESTÃO DE ORDENS
# ==========================================
@app.post("/webhook/order")
def receive_order(payload: OrderPayload, db: Session = Depends(get_db)):
    try:
        raw_ticker = payload.ticker.strip().upper()
        raw_option = payload.option.strip().upper()
        raw_status = (payload.status or "FILLED").strip().upper()
        raw_action = (payload.action or "EXECUTION").strip().upper()

        letra_op = raw_option[4].upper() if len(raw_option) >= 5 else ""
        tipo_op = "CALL" if letra_op in "ABCDEFGHIJKL" else ("PUT" if letra_op in "MNOPQRSTUVWX" else payload.option_type)

        lote = payload.lots if payload.lots > 0 else 100
        preco_exec = payload.real_price if (payload.real_price and payload.real_price > 0) else payload.price
        inv_cap = payload.invested_capital if (payload.invested_capital and payload.invested_capital > 0) else (lote * preco_exec)

        # 1. Se for comando ou confirmação de encerramento remoto
        if raw_action in ("EXIT_REQUESTED", "CLOSE", "CLOSED", "EXIT") or raw_status in ("CLOSED", "EXIT_REQUESTED"):
            pos_existente = db.query(OrderModel).filter(
                OrderModel.option_name == raw_option,
                OrderModel.status.in_(["FILLED", "CLOSE_REQUESTED", "SUBMITTED"])
            ).first()

            if pos_existente:
                pos_existente.status = "CLOSED"
                pos_existente.closed_at = datetime.now()
                if preco_exec > 0:
                    pos_existente.current_price = preco_exec
                    pos_existente.pnl_brl = (preco_exec - pos_existente.real_price) * pos_existente.lots
                    pos_existente.pnl_perc = ((preco_exec - pos_existente.real_price) / pos_existente.real_price * 100.0) if pos_existente.real_price > 0 else 0.0
                if payload.error_message:
                    pos_existente.error_message = payload.error_message
                db.commit()
                return {"status": "success", "message": f"Posição {raw_option} marcada como ENCERRADA.", "order_id": pos_existente.id}

        # 2. Se for atualização de ordem existente para FILLED
        ordem_existente = db.query(OrderModel).filter(
            OrderModel.option_name == raw_option,
            OrderModel.status == "SUBMITTED"
        ).first()

        if ordem_existente and raw_status == "FILLED":
            ordem_existente.status = "FILLED"
            ordem_existente.real_price = preco_exec
            ordem_existente.current_price = preco_exec
            ordem_existente.invested_capital = inv_cap
            db.commit()
            return {"status": "success", "message": f"Ordem {raw_option} atualizada para FILLED.", "order_id": ordem_existente.id}

        # 3. Trava de Duplicação Estrita para Posições Abertas
        if raw_status in ("FILLED", "SUBMITTED"):
            em_aberto = db.query(OrderModel).filter(
                OrderModel.option_name == raw_option,
                OrderModel.status.in_(["FILLED", "SUBMITTED"])
            ).first()
            if em_aberto:
                return {"status": "ignored", "message": f"Posição ativa #{em_aberto.id} já existe para {raw_option}."}

        # 4. Criar Novo Registro
        db_order = OrderModel(
            timestamp=payload.timestamp or datetime.now().isoformat(),
            ticker=raw_ticker,
            option_name=raw_option,
            price=payload.price,
            real_price=preco_exec,
            current_price=preco_exec,
            option_type=tipo_op,
            strike=payload.strike,
            spot_price=payload.spot_price,
            direction=payload.direction,
            action=payload.action,
            magic=payload.magic,
            lots=lote,
            sl_perc=payload.sl_perc,
            tp_perc=payload.tp_perc,
            analysis=payload.analysis,
            order_type=payload.order_type,
            status=raw_status,
            error_message=payload.error_message,
            invested_capital=inv_cap,
            fees=payload.fees,
            expiration=payload.expiration,
            account_id=payload.account_id or os.getenv("FENIX_ACCOUNT", "70960268")
        )
        db.add(db_order)
        db.commit()
        db.refresh(db_order)
        return {"status": "success", "message": f"Ordem {raw_option} registrada com sucesso no OMS.", "order_id": db_order.id}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/market-update")
def update_market_quotes(payload: MarketUpdatePayload, db: Session = Depends(get_db)):
    """Atualiza o preço atual de mercado (Mark-to-Market) de todas as opções ativas."""
    updated = 0
    for item in payload.quotes:
        pos = db.query(OrderModel).filter(
            OrderModel.option_name == item.option.strip().upper(),
            OrderModel.status == "FILLED"
        ).first()
        if pos and item.current_price > 0:
            pos.current_price = item.current_price
            pos.pnl_brl = (item.current_price - pos.real_price) * pos.lots
            pos.pnl_perc = ((item.current_price - pos.real_price) / pos.real_price * 100.0) if pos.real_price > 0 else 0.0
            updated += 1
    db.commit()
    return {"status": "success", "updated_quotes": updated}

@app.get("/api/commands/pending")
def get_pending_commands(db: Session = Depends(get_db)):
    """Rota consumida pelo robô para saber se o cliente pediu para fechar ordens pelo OMS."""
    pedidos_fechamento = db.query(OrderModel).filter(OrderModel.status == "CLOSE_REQUESTED").all()
    comandos = [{"order_id": o.id, "option": o.option_name, "action": "CLOSE", "volume": o.lots} for o in pedidos_fechamento]
    return comandos

@app.post("/api/orders/{order_id}/close")
def close_order_web(order_id: int, db: Session = Depends(get_db)):
    order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Ordem não encontrada.")
    order.status = "CLOSE_REQUESTED"
    order.error_message = "Solicitação de zeragem manual via Fenix OMS Web"
    db.commit()
    return {"status": "success", "message": f"Solicitação de zeragem para #{order_id} enviada ao robô."}

@app.post("/api/orders/close-all")
def close_all_orders_web(db: Session = Depends(get_db)):
    abertas = db.query(OrderModel).filter(OrderModel.status == "FILLED").all()
    count = len(abertas)
    for o in abertas:
        o.status = "CLOSE_REQUESTED"
        o.error_message = "Zeragem coletiva solicitada pelo Painel OMS"
    db.commit()
    return {"status": "success", "message": f"Comando de encerramento enviado para {count} posições."}

@app.post("/api/orders/reset-db")
def reset_database(db: Session = Depends(get_db)):
    db.query(OrderModel).delete()
    db.commit()
    return {"status": "success", "message": "Histórico do banco de dados zerado com sucesso."}

# ==========================================
# 4. CONSULTAS E MÉTRICAS DA CONTA
# ==========================================
@app.get("/api/orders")
def get_orders(db: Session = Depends(get_db)):
    orders = db.query(OrderModel).order_by(desc(OrderModel.id)).limit(150).all()
    return orders

@app.get("/api/account-status")
def get_account_status(db: Session = Depends(get_db)):
    uptime = datetime.now() - START_TIME
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    abertas = db.query(OrderModel).filter(OrderModel.status == "FILLED").all()
    fechadas = db.query(OrderModel).filter(OrderModel.status == "CLOSED").all()

    total_investido = sum(o.invested_capital for o in abertas)
    lucro_aberto = sum(o.pnl_brl for o in abertas if o.pnl_brl > 0)
    perda_aberta = sum(o.pnl_brl for o in abertas if o.pnl_brl < 0)
    saldo_aberto = lucro_aberto + perda_aberta

    lucro_fechado = sum(o.pnl_brl for o in fechadas)
    win_trades = len([o for o in fechadas if o.pnl_brl > 0])
    win_rate = (win_trades / len(fechadas) * 100.0) if fechadas else 0.0

    banca_base = float(os.getenv("FENIX_INITIAL_BALANCE", "5000.0"))
    equity_total = banca_base + lucro_fechado + saldo_aberto

    return {
        "account_id": os.getenv("FENIX_ACCOUNT", "70960268"),
        "broker": "Nelogica ProfitPro (B3 Engine)",
        "equity": equity_total,
        "available_margin": max(0.0, equity_total - total_investado_capital if (total_investado_capital := total_investido) else equity_total),
        "total_invested": total_investido,
        "lucro_aberto": lucro_aberto,
        "perda_aberta": perda_aberta,
        "saldo_aberto": saldo_aberto,
        "lucro_fechado": lucro_fechado,
        "win_rate": win_rate,
        "total_active": len(abertas),
        "total_closed": len(fechadas),
        "dll_status": "Conectado e Operacional",
        "uptime": f"{hours}h {minutes}m",
        "server_time": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    }

# ==========================================
# 5. ROTAS DE BACKUP E EXPORTAÇÃO
# ==========================================
@app.get("/api/backup/export")
def export_backup(db: Session = Depends(get_db)):
    orders = db.query(OrderModel).all()
    data = [{
        "timestamp": o.timestamp, "ticker": o.ticker, "option_name": o.option_name,
        "price": o.price, "real_price": o.real_price, "current_price": o.current_price,
        "option_type": o.option_type, "strike": o.strike, "spot_price": o.spot_price,
        "direction": o.direction, "action": o.action, "magic": o.magic, "lots": o.lots,
        "sl_perc": o.sl_perc, "tp_perc": o.tp_perc, "analysis": o.analysis,
        "order_type": o.order_type, "status": o.status, "error_message": o.error_message,
        "invested_capital": o.invested_capital, "fees": o.fees, "pnl_brl": o.pnl_brl,
        "pnl_perc": o.pnl_perc, "expiration": o.expiration, "account_id": o.account_id,
        "created_at": o.created_at.isoformat() if o.created_at else None
    } for o in orders]

    content = {"version": "2.0.0", "exported_at": datetime.now().isoformat(), "orders": data}
    filename = f"fenix_oms_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return JSONResponse(content=content, headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.post("/api/backup/restore")
async def restore_backup(file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        raw_bytes = await file.read()
        data = json.loads(raw_bytes.decode("utf-8")).get("orders", [])
        restored = 0
        for item in data:
            db_order = OrderModel(
                timestamp=item.get("timestamp"), ticker=item.get("ticker"),
                option_name=item.get("option_name"), price=item.get("price", 0.0),
                real_price=item.get("real_price", 0.0), current_price=item.get("current_price", 0.0),
                option_type=item.get("option_type"), strike=item.get("strike", 0.0),
                spot_price=item.get("spot_price", 0.0), direction=item.get("direction", "BUY"),
                action=item.get("action", "EXECUTION"), magic=item.get("magic", 123456),
                lots=item.get("lots", 100), sl_perc=item.get("sl_perc", 20.0),
                tp_perc=item.get("tp_perc", 100.0), analysis=item.get("analysis"),
                order_type=item.get("order_type", "A MERCADO"), status=item.get("status", "FILLED"),
                error_message=item.get("error_message"), invested_capital=item.get("invested_capital", 0.0),
                pnl_brl=item.get("pnl_brl", 0.0), pnl_perc=item.get("pnl_perc", 0.0),
                expiration=item.get("expiration"), account_id=item.get("account_id")
            )
            db.add(db_order)
            restored += 1
        db.commit()
        return {"status": "success", "message": f"{restored} ordens restauradas com sucesso!"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

# ==========================================
# 6. DASHBOARD PROFISSIONAL INTEGRADO (HTML5 + TAILWIND)
# ==========================================
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content="""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>FENIX OMS - Institutional Portfolio & Execution Manager</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <script>
            let currentTab = 'active';

            async function loadOMSData() {
                try {
                    const resAcc = await fetch('/api/account-status');
                    const acc = await resAcc.json();

                    document.getElementById('acc-id').innerText = acc.account_id;
                    document.getElementById('acc-equity').innerText = 'R$ ' + acc.equity.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('acc-invested').innerText = 'R$ ' + acc.total_invested.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('acc-closed-profit').innerText = 'R$ ' + acc.lucro_fechado.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('acc-winrate').innerText = acc.win_rate.toFixed(1) + '%';
                    document.getElementById('acc-uptime').innerText = acc.uptime;

                    const elSaldo = document.getElementById('stat-saldo-aberto');
                    elSaldo.innerText = 'R$ ' + acc.saldo_aberto.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    elSaldo.className = acc.saldo_aberto >= 0 ? "text-lg font-bold text-emerald-400" : "text-lg font-bold text-rose-500";

                    document.getElementById('stat-lucro-aberto').innerText = '+R$ ' + acc.lucro_aberto.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('stat-perda-aberta').innerText = '-R$ ' + Math.abs(acc.perda_aberta).toLocaleString('pt-BR', {minimumFractionDigits: 2});

                    const resOrders = await fetch('/api/orders');
                    const orders = await resOrders.json();
                    renderOrders(orders);
                } catch (e) {
                    console.error('Falha de conexão com o OMS:', e);
                }
            }

            function renderOrders(orders) {
                const tbody = document.getElementById('orders-table-body');
                tbody.innerHTML = '';

                let filtered = [];
                if (currentTab === 'active') {
                    filtered = orders.filter(o => o.status === 'FILLED' || o.status === 'SUBMITTED' || o.status === 'CLOSE_REQUESTED');
                } else if (currentTab === 'closed') {
                    filtered = orders.filter(o => o.status === 'CLOSED');
                } else {
                    filtered = orders.filter(o => o.status === 'REJECTED' || o.error_message);
                }

                document.getElementById('badge-active').innerText = orders.filter(o => o.status === 'FILLED').length;
                document.getElementById('badge-closed').innerText = orders.filter(o => o.status === 'CLOSED').length;
                document.getElementById('badge-audit').innerText = orders.filter(o => o.status === 'REJECTED' || o.error_message).length;

                if (filtered.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="12" class="text-center py-10 text-gray-500 italic">Nenhum registro encontrado nesta categoria.</td></tr>`;
                    return;
                }

                filtered.forEach(o => {
                    const isCall = o.option_type === 'CALL';
                    const badgeType = isCall ?
                        `<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-blue-500/10 text-blue-400 border border-blue-500/30">CALL</span>` :
                        `<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/10 text-amber-400 border border-amber-500/30">PUT</span>`;

                    const dataFormatada = new Date(o.created_at).toLocaleString('pt-BR', {day: '2-digit', month: '2-digit', hour: '2-digit', minute:'2-digit'});
                    const vencTxt = o.expiration ? o.expiration : '3ª SEXTA';

                    const pCompra = o.real_price > 0 ? o.real_price : o.price;
                    const pAtual = o.current_price > 0 ? o.current_price : pCompra;
                    const pnlBrl = o.pnl_brl || ((pAtual - pCompra) * o.lots);
                    const pnlPerc = o.pnl_perc || (((pAtual - pCompra) / pCompra) * 100);

                    const pnlClass = pnlBrl >= 0 ? "text-emerald-400 font-bold" : "text-rose-500 font-bold";

                    let actionHtml = '';
                    if (o.status === 'FILLED') {
                        actionHtml = `<button onclick="closeSingle(${o.id})" class="px-2.5 py-1 bg-rose-600/80 hover:bg-rose-600 text-white font-bold text-xs rounded transition shadow">Zerar</button>`;
                    } else if (o.status === 'CLOSE_REQUESTED') {
                        actionHtml = `<span class="text-amber-400 text-xs font-semibold animate-pulse">Zerando...</span>`;
                    } else if (o.status === 'CLOSED') {
                        actionHtml = `<span class="text-gray-500 text-xs"><i class="fa-solid fa-check"></i> Encerrado</span>`;
                    } else {
                        actionHtml = `<span class="text-rose-400 text-xs">${o.error_message || 'Rejeitada'}</span>`;
                    }

                    const row = `
                        <tr class="border-b border-gray-800/60 hover:bg-gray-800/30 transition text-xs">
                            <td class="py-3 px-3 text-gray-400 font-mono">${dataFormatada}</td>
                            <td class="py-3 px-3 font-bold text-white tracking-wide">${o.ticker}</td>
                            <td class="py-3 px-3 font-mono text-cyan-400 font-bold">${o.option_name}</td>
                            <td class="py-3 px-3 text-gray-300 font-mono">${vencTxt}</td>
                            <td class="py-3 px-3">${badgeType}</td>
                            <td class="py-3 px-3 text-yellow-400 font-mono font-bold">${o.lots}</td>
                            <td class="py-3 px-3 text-gray-200 font-mono">R$ ${pCompra.toFixed(2)}</td>
                            <td class="py-3 px-3 text-cyan-300 font-mono font-bold">R$ ${pAtual.toFixed(2)}</td>
                            <td class="py-3 px-3 text-gray-300 font-mono">R$ ${o.invested_capital.toFixed(2)}</td>
                            <td class="py-3 px-3 ${pnlClass}">${pnlPerc >= 0 ? '+' : ''}${pnlPerc.toFixed(2)}%</td>
                            <td class="py-3 px-3 ${pnlClass}">R$ ${pnlBrl >= 0 ? '+' : ''}${pnlBrl.toFixed(2)}</td>
                            <td class="py-3 px-3 text-center">${actionHtml}</td>
                        </tr>
                    `;
                    tbody.innerHTML += row;
                });
            }

            function setTab(tab) {
                currentTab = tab;
                document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('border-blue-500', 'text-blue-400'));
                document.getElementById(`tab-${tab}`).classList.add('border-blue-500', 'text-blue-400');
                loadOMSData();
            }

            async function closeSingle(id) {
                if (!confirm(`Deseja solicitar a ZERAGEM da posição #${id} no ProfitPro?`)) return;
                const res = await fetch(`/api/orders/${id}/close`, { method: 'POST' });
                if (res.ok) loadOMSData();
            }

            async function closeAll() {
                if (!confirm('ATENÇÃO: Deseja solicitar a ZERAGEM DE TODAS as posições abertas na corretora?')) return;
                const res = await fetch('/api/orders/close-all', { method: 'POST' });
                if (res.ok) loadOMSData();
            }

            function triggerBackup() {
                window.location.href = '/api/backup/export';
            }

            async function handleRestore(event) {
                const file = event.target.files[0];
                if (!file) return;
                if (!confirm(`Restaurar o banco de dados utilizando "${file.name}"?`)) return;

                const formData = new FormData();
                formData.append('file', file);
                const res = await fetch('/api/backup/restore', { method: 'POST', body: formData });
                if (res.ok) {
                    alert('Backup restaurado com sucesso!');
                    loadOMSData();
                } else {
                    alert('Falha ao restaurar arquivo.');
                }
            }

            setInterval(loadOMSData, 3000);
            window.onload = () => {
                setTab('active');
                loadOMSData();
            };
        </script>
    </head>
    <body class="bg-[#0b0f19] text-gray-100 font-sans antialiased min-h-screen p-4 md:p-8">
        <div class="max-w-7xl mx-auto space-y-6">

            <!-- HEADER PRINCIPAL -->
            <div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 bg-gray-900/60 border border-gray-800 p-5 rounded-2xl backdrop-blur-xl shadow-2xl">
                <div>
                    <div class="flex items-center gap-3">
                        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-blue-600 to-cyan-400 flex items-center justify-center shadow-lg shadow-blue-500/20">
                            <i class="fa-solid fa-chart-line text-white text-lg"></i>
                        </div>
                        <div>
                            <h1 class="text-xl font-black tracking-wider text-white">FENIX OMS <span class="text-xs px-2.5 py-1 rounded-md bg-blue-500/20 text-blue-400 font-mono border border-blue-500/30">PROFITPRO LIVE</span></h1>
                            <p class="text-xs text-gray-400 mt-0.5">Gestão de Ordens, Risco em Tempo Real e Auditoria da Carteira B3</p>
                        </div>
                    </div>
                </div>

                <div class="flex items-center gap-2 flex-wrap">
                    <button onclick="triggerBackup()" class="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 text-gray-200 text-xs font-semibold rounded-lg border border-gray-700 transition flex items-center gap-1.5 shadow-sm">
                        <i class="fa-solid fa-download"></i> Backup
                    </button>
                    <label class="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 text-gray-200 text-xs font-semibold rounded-lg border border-gray-700 transition flex items-center gap-1.5 cursor-pointer shadow-sm">
                        <i class="fa-solid fa-upload"></i> Restaurar
                        <input type="file" accept=".json" onchange="handleRestore(event)" class="hidden">
                    </label>
                    <button onclick="closeAll()" class="px-3.5 py-1.5 bg-rose-600 hover:bg-rose-500 text-white text-xs font-bold rounded-lg transition shadow-lg shadow-rose-600/30 flex items-center gap-1.5">
                        <i class="fa-solid fa-triangle-exclamation"></i> Zerar Todas
                    </button>
                    <div class="flex items-center gap-2 pl-3 border-l border-gray-800">
                        <span class="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></span>
                        <span class="text-xs text-emerald-400 font-bold font-mono">LIVE FEED</span>
                    </div>
                </div>
            </div>

            <!-- CARDS DE MÉTRICAS INSTITUCIONAIS -->
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
                <div class="bg-gray-900/60 border border-gray-800 p-4 rounded-xl">
                    <span class="text-[11px] font-bold text-gray-400 uppercase tracking-wider block">Conta Corretora</span>
                    <span id="acc-id" class="text-xl font-black text-white font-mono mt-1 block">---</span>
                    <span class="text-[11px] text-gray-500 mt-1 block">Nelogica ProfitPro B3</span>
                </div>

                <div class="bg-gray-900/60 border border-gray-800 p-4 rounded-xl">
                    <span class="text-[11px] font-bold text-gray-400 uppercase tracking-wider block">Patrimônio / Equity</span>
                    <span id="acc-equity" class="text-xl font-black text-emerald-400 font-mono mt-1 block">R$ 0,00</span>
                    <span class="text-[11px] text-gray-500 mt-1 block">Alocado: <span id="acc-invested" class="text-gray-300 font-semibold">R$ 0,00</span></span>
                </div>

                <div class="bg-gray-900/60 border border-gray-800 p-4 rounded-xl">
                    <span class="text-[11px] font-bold text-gray-400 uppercase tracking-wider block">Resultado Aberto (PnL)</span>
                    <span id="stat-saldo-aberto" class="text-xl font-black font-mono mt-1 block">R$ 0,00</span>
                    <div class="flex justify-between text-[10px] text-gray-400 mt-1">
                        <span id="stat-lucro-aberto" class="text-emerald-400">+R$ 0,00</span>
                        <span id="stat-perda-aberta" class="text-rose-400">-R$ 0,00</span>
                    </div>
                </div>

                <div class="bg-gray-900/60 border border-gray-800 p-4 rounded-xl">
                    <span class="text-[11px] font-bold text-gray-400 uppercase tracking-wider block">Resultado Fechado</span>
                    <span id="acc-closed-profit" class="text-xl font-black text-cyan-400 font-mono mt-1 block">R$ 0,00</span>
                    <span class="text-[11px] text-gray-500 mt-1 block">Win Rate: <span id="acc-winrate" class="text-white font-semibold">0%</span></span>
                </div>

                <div class="bg-gray-900/60 border border-gray-800 p-4 rounded-xl">
                    <span class="text-[11px] font-bold text-gray-400 uppercase tracking-wider block">Uptime do Servidor</span>
                    <span id="acc-uptime" class="text-lg font-bold text-purple-300 font-mono mt-1 block">0h 0m</span>
                    <span class="text-[11px] text-emerald-400 font-semibold mt-1 block"><i class="fa-solid fa-bolt"></i> OMS 100% Online</span>
                </div>
            </div>

            <!-- TABELA COM ABAS -->
            <div class="bg-gray-900/60 border border-gray-800 rounded-2xl overflow-hidden shadow-2xl">
                <div class="border-b border-gray-800 px-6 pt-4 flex gap-6 text-sm font-bold">
                    <button id="tab-active" onclick="setTab('active')" class="tab-btn pb-3 border-b-2 border-transparent text-gray-400 hover:text-white transition flex items-center gap-2">
                        <i class="fa-solid fa-chart-pie"></i> Posições em Custódia <span id="badge-active" class="px-2 py-0.5 rounded-full text-[10px] bg-blue-500/20 text-blue-400">0</span>
                    </button>
                    <button id="tab-closed" onclick="setTab('closed')" class="tab-btn pb-3 border-b-2 border-transparent text-gray-400 hover:text-white transition flex items-center gap-2">
                        <i class="fa-solid fa-clock-rotate-left"></i> Histórico de Encerramentos <span id="badge-closed" class="px-2 py-0.5 rounded-full text-[10px] bg-emerald-500/20 text-emerald-400">0</span>
                    </button>
                    <button id="tab-audit" onclick="setTab('audit')" class="tab-btn pb-3 border-b-2 border-transparent text-gray-400 hover:text-white transition flex items-center gap-2">
                        <i class="fa-solid fa-shield-halved"></i> Auditoria / Rejeições <span id="badge-audit" class="px-2 py-0.5 rounded-full text-[10px] bg-rose-500/20 text-rose-400">0</span>
                    </button>
                </div>

                <div class="overflow-x-auto">
                    <table class="w-full text-left border-collapse">
                        <thead>
                            <tr class="bg-gray-950/40 text-gray-400 uppercase text-[11px] tracking-wider border-b border-gray-800">
                                <th class="py-3 px-3">Data</th>
                                <th class="py-3 px-3">Ativo</th>
                                <th class="py-3 px-3">Opção</th>
                                <th class="py-3 px-3">Vencimento</th>
                                <th class="py-3 px-3">Tipo</th>
                                <th class="py-3 px-3">Lote</th>
                                <th class="py-3 px-3">P. Entrada</th>
                                <th class="py-3 px-3">P. Mercado</th>
                                <th class="py-3 px-3">Investido</th>
                                <th class="py-3 px-3">Lucro (%)</th>
                                <th class="py-3 px-3">Resultado (R$)</th>
                                <th class="py-3 px-3 text-center">Ação</th>
                            </tr>
                        </thead>
                        <tbody id="orders-table-body" class="divide-y divide-gray-800/40">
                            <!-- Injetado via Javascript -->
                        </tbody>
                    </table>
                </div>
            </div>

        </div>
    </body>
    </html>
    """)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
