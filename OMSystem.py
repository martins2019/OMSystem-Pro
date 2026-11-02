from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
import uvicorn
import os
import sqlite3
import json

# ==========================================
# 1. CONFIGURAÇÃO SEGURA DO BANCO DE DADOS
# ==========================================
# Lê a variável de ambiente, garantindo fallback seguro para SQLite caso venha vazia ou nula
raw_db_url = os.getenv("DATABASE_URL", "").strip()

if not raw_db_url:
    DATABASE_URL = "sqlite:///./orion_oms.db"
else:
    # Correção para o Render/PostgreSQL: SQLAlchemy exige 'postgresql://' em vez de 'postgres://'
    if raw_db_url.startswith("postgres://"):
        DATABASE_URL = raw_db_url.replace("postgres://", "postgresql://", 1)
    else:
        DATABASE_URL = raw_db_url

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class OrderModel(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(String, index=True)
    ticker = Column(String, index=True)
    option_name = Column(String)
    price = Column(Float)
    real_price = Column(Float)
    option_type = Column(String)
    strike = Column(Float)
    spot_price = Column(Float)
    direction = Column(String)
    action = Column(String)
    magic = Column(Integer)
    lots = Column(Integer)
    sl_perc = Column(Float)
    tp_perc = Column(Float)
    analysis = Column(String)
    order_type = Column(String)
    status = Column(String, default="FILLED") # PENDING, FILLED, REJECTED, CLOSED
    error_message = Column(String, nullable=True) 
    invested_capital = Column(Float, default=0.0) 
    fees = Column(Float, default=0.0) 
    created_at = Column(DateTime, default=datetime.now)

Base.metadata.create_all(bind=engine)

def check_and_migrate_db():
    if "sqlite" in DATABASE_URL:
        db_path = "orion_oms.db"
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("SELECT error_message FROM orders LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE orders ADD COLUMN error_message TEXT")
                print("[MIGRAÇÃO] Coluna 'error_message' adicionada com sucesso!")

            try:
                cursor.execute("SELECT invested_capital FROM orders LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE orders ADD COLUMN invested_capital FLOAT DEFAULT 0.0")
                print("[MIGRAÇÃO] Coluna 'invested_capital' adicionada com sucesso!")

            try:
                cursor.execute("SELECT fees FROM orders LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE orders ADD COLUMN fees FLOAT DEFAULT 0.0")
                print("[MIGRAÇÃO] Coluna 'fees' adicionada com sucesso!")

            conn.commit()
            conn.close()

check_and_migrate_db()

app = FastAPI(
    title="Fenix OMS - Order Management System & ProfitPro Auditor",
    version="1.8.7"
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
    real_price: float
    option_type: str
    strike: float
    spot_price: float
    direction: str
    action: str
    magic: int
    lots: int
    sl_perc: float
    tp_perc: float
    analysis: Optional[str] = ""
    order_type: str
    status: Optional[str] = "FILLED"
    error_message: Optional[str] = None
    invested_capital: Optional[float] = 0.0
    fees: Optional[float] = 0.0

# ==========================================
# 2. ENDPOINTS DE WEBHOOK E GESTÃO DE DADOS
# ==========================================
@app.post("/webhook/order")
def receive_order(payload: OrderPayload, db: Session = Depends(get_db)):
    try:
        raw_ticker = payload.ticker.strip().upper()
        raw_direction = payload.option_type.strip().upper()

        if payload.status == "FILLED":
            existing = db.query(OrderModel).filter(
                OrderModel.ticker == raw_ticker,
                OrderModel.status == "FILLED"
            ).first()

            if existing:
                return {
                    "status": "ignored",
                    "message": f"Ordem duplicada/bloqueada pelo OMS para o ativo base {raw_ticker}. Já existe posição ativa #{existing.id} ({existing.option_name}).",
                    "existing_id": existing.id
                }

        lote_real = payload.lots if payload.lots > 0 else 100
        inv_cap = payload.invested_capital
        if inv_cap is None or inv_cap <= 0.0:
            inv_cap = lote_real * payload.real_price

        db_order = OrderModel(
            timestamp=payload.timestamp or datetime.now().isoformat(),
            ticker=raw_ticker,
            option_name=payload.option.strip().upper(),
            price=payload.price,
            real_price=payload.real_price,
            option_type=raw_direction,
            strike=payload.strike,
            spot_price=payload.spot_price,
            direction=payload.direction,
            action=payload.action,
            magic=payload.magic,
            lots=lote_real,
            sl_perc=payload.sl_perc,
            tp_perc=payload.tp_perc,
            analysis=payload.analysis,
            order_type=payload.order_type,
            status=payload.status,
            error_message=payload.error_message,
            invested_capital=inv_cap,
            fees=payload.fees
        )
        db.add(db_order)
        db.commit()
        db.refresh(db_order)
        return {"status": "success", "message": "Ordem registrada com sucesso no OMS ProfitPro (Conta 70960268)", "order_id": db_order.id}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook_results.php")
def receive_telemetry(data: dict, db: Session = Depends(get_db)):
    try:
        ticket = data.get("ticket")
        if ticket:
            order = db.query(OrderModel).filter(OrderModel.id == ticket).first()
            if order:
                order.status = "CLOSED"
                db.commit()
        return {"status": "success", "telemetry": "received"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/orders/{order_id}/close")
def close_order_manual(order_id: int, db: Session = Depends(get_db)):
    order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Ordem não encontrada.")
    order.status = "CLOSED"
    order.error_message = "Fechamento manual via painel OMS ProfitPro (Conta 70960268)"
    db.commit()
    return {"status": "success", "message": f"Ordem {order_id} encerrada com sucesso."}

@app.post("/api/orders/close-all")
def close_all_orders_manual(db: Session = Depends(get_db)):
    try:
        filled_orders = db.query(OrderModel).filter(OrderModel.status == "FILLED").all()
        count = len(filled_orders)
        for order in filled_orders:
            order.status = "CLOSED"
            order.error_message = "Fechamento global via painel OMS ProfitPro (Conta 70960268)"
        db.commit()
        return {"status": "success", "message": f"{count} ordens encerradas com sucesso."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/orders/reset-db")
def reset_database(db: Session = Depends(get_db)):
    db.query(OrderModel).delete()
    db.commit()
    return {"status": "success", "message": "Banco de dados limpo com sucesso."}

# ==========================================
# 3. ROTAS DE BACKUP E RESTAURAÇÃO
# ==========================================
@app.get("/api/backup/export")
def export_backup(db: Session = Depends(get_db)):
    """Exporta todos os registros do banco em formato JSON para backup seguro."""
    orders = db.query(OrderModel).all()
    data = []
    for o in orders:
        data.append({
            "timestamp": o.timestamp,
            "ticker": o.ticker,
            "option_name": o.option_name,
            "price": o.price,
            "real_price": o.real_price,
            "option_type": o.option_type,
            "strike": o.strike,
            "spot_price": o.spot_price,
            "direction": o.direction,
            "action": o.action,
            "magic": o.magic,
            "lots": o.lots,
            "sl_perc": o.sl_perc,
            "tp_perc": o.tp_perc,
            "analysis": o.analysis,
            "order_type": o.order_type,
            "status": o.status,
            "error_message": o.error_message,
            "invested_capital": o.invested_capital,
            "fees": o.fees,
            "created_at": o.created_at.isoformat() if o.created_at else None
        })
    return JSONResponse(content={"version": "1.8.7", "exported_at": datetime.now().isoformat(), "orders": data})

@app.post("/api/backup/restore")
async def restore_backup(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Restaura o banco de dados através de um arquivo JSON de backup enviado."""
    try:
        content = await file.read()
        backup_data = json.loads(content.decode("utf-8"))
        orders_list = backup_data.get("orders", [])
        
        restored_count = 0
        for item in orders_list:
            exists = db.query(OrderModel).filter(
                OrderModel.ticker == item.get("ticker"),
                OrderModel.timestamp == item.get("timestamp"),
                OrderModel.status == item.get("status")
            ).first()

            if not exists:
                dt_created = datetime.now()
                if item.get("created_at"):
                    try:
                        dt_created = datetime.fromisoformat(item.get("created_at"))
                    except:
                        pass

                new_order = OrderModel(
                    timestamp=item.get("timestamp"),
                    ticker=item.get("ticker"),
                    option_name=item.get("option_name"),
                    price=item.get("price"),
                    real_price=item.get("real_price"),
                    option_type=item.get("option_type"),
                    strike=item.get("strike"),
                    spot_price=item.get("spot_price"),
                    direction=item.get("direction"),
                    action=item.get("action"),
                    magic=item.get("magic"),
                    lots=item.get("lots"),
                    sl_perc=item.get("sl_perc"),
                    tp_perc=item.get("tp_perc"),
                    analysis=item.get("analysis"),
                    order_type=item.get("order_type"),
                    status=item.get("status", "FILLED"),
                    error_message=item.get("error_message"),
                    invested_capital=item.get("invested_capital", 0.0),
                    fees=item.get("fees", 0.0),
                    created_at=dt_created
                )
                db.add(new_order)
                restored_count += 1

        db.commit()
        return {"status": "success", "message": f"Backup restaurado com sucesso! {restored_count} ordens importadas."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao processar arquivo de backup: {str(e)}")

# ==========================================
# 4. APIs DE CONSULTA E STATUS DA CONTA
# ==========================================
@app.get("/api/orders")
def get_orders(db: Session = Depends(get_db)):
    all_orders = db.query(OrderModel).order_by(OrderModel.created_at.desc()).all()
    seen_keys = set()
    unique_orders = []

    for order in all_orders:
        key = f"{order.ticker}_{order.status}"
        if key not in seen_keys:
            seen_keys.add(key)
            unique_orders.append(order)

    return unique_orders[:100]

@app.get("/api/account-status")
def get_account_status(db: Session = Depends(get_db)):
    uptime = datetime.now() - START_TIME
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    orders = db.query(OrderModel).all()
    filled_orders = [o for o in orders if o.status == 'FILLED']
    total_invested = sum([o.invested_capital for o in filled_orders])
    total_fees = sum([o.fees for o in filled_orders])

    lucro_aberto = 0.0
    perda_aberta = 0.0
    saldo_aberto = lucro_aberto + perda_aberta

    return {
        "account_id": "70960268",
        "broker": "B3 (ProfitPro Engine / DLL)",
        "equity": 15420.50 - total_fees,
        "available_margin": 12500.00 - total_invested,
        "total_invested": total_invested,
        "total_fees": total_fees,
        "lucro_aberto": lucro_aberto,
        "perda_aberta": perda_aberta,
        "saldo_aberto": saldo_aberto,
        "dll_status": "Conectado e Operacional",
        "connection_mode": "PROFITPRO_CONNECTOR",
        "uptime": f"{hours}h {minutes}m",
        "server_time": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    }

# ==========================================
# 5. DASHBOARD WEB EM TEMPO REAL
# ==========================================
@app.get("/", response_class=HTMLResponse)
def dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <title>FENIX OMS - Painel de Auditoria & Execução ProfitPro (Conta 70960268)</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script>
            function obterDataVencimentoBr(tickerOpcao, dataCriacaoIso) {
                if (!tickerOpcao || tickerOpcao.length < 5) return "--/--/----";
                
                const letra = tickerOpcao.charAt(4).toUpperCase();
                const codigosCall = ['A','B','C','D','E','F','G','H','I','J','K','L'];
                const codigosPut  = ['M','N','O','P','Q','R','S','T','U','V','W','X'];

                let mesIndex = codigosCall.indexOf(letra);
                if (mesIndex === -1) {
                    mesIndex = codigosPut.indexOf(letra);
                }

                if (mesIndex === -1) return "--/--/----";

                const dtCriacao = dataCriacaoIso ? new Date(dataCriacaoIso) : new Date();
                let ano = dtCriacao.getFullYear();
                let mes = mesIndex;

                if (mes < dtCriacao.getMonth()) {
                    ano += 1;
                }

                let sextasEncontradas = 0;
                let diaVencimento = null;

                for (let d = 1; d <= 31; d++) {
                    let testeData = new Date(ano, mes, d);
                    if (testeData.getMonth() !== mes) break;
                    if (testeData.getDay() === 5) {
                        sextasEncontradas++;
                        if (sextasEncontradas === 3) {
                            diaVencimento = d;
                            break;
                        }
                    }
                }

                if (!diaVencimento) return "--/--/----";

                let strDia = String(diaVencimento).padStart(2, '0');
                let strMes = String(mes + 1).padStart(2, '0');
                return `${strDia}/${strMes}/${ano}`;
            }

            async function fetchOMSData() {
                try {
                    const resAccount = await fetch('/api/account-status');
                    const acc = await resAccount.json();
                    
                    document.getElementById('acc-id').innerText = acc.account_id;
                    document.getElementById('acc-equity').innerText = 'R$ ' + acc.equity.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('acc-invested').innerText = 'R$ ' + (acc.total_invested || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('acc-broker').innerText = acc.broker;
                    document.getElementById('acc-dll').innerText = acc.dll_status;
                    document.getElementById('acc-uptime').innerText = acc.uptime;

                    const resOrders = await fetch('/api/orders');
                    const orders = await resOrders.json();
                    const tbody = document.getElementById('orders-table');
                    tbody.innerHTML = '';
                    
                    document.getElementById('stat-total').innerText = orders.length;
                    document.getElementById('stat-filled').innerText = orders.filter(o => o.status === 'FILLED').length;
                    document.getElementById('stat-rejected').innerText = orders.filter(o => o.status === 'REJECTED').length;

                    let lucroAbertoTotal = 0.0;
                    let perdaAbertaTotal = 0.0;

                    orders.forEach(o => {
                        if (o.status === 'FILLED') {
                            let pnlOp = 0.0;
                            if (pnlOp > 0) lucroAbertoTotal += pnlOp;
                            else perdaAbertaTotal += pnlOp;
                        }
                    });

                    let saldoAbertoTotal = lucroAbertoTotal + perdaAbertaTotal;
                    
                    document.getElementById('stat-lucro-aberto').innerText = 'R$ ' + lucroAbertoTotal.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    document.getElementById('stat-perda-aberta').innerText = 'R$ ' + perdaAbertaTotal.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    
                    const elSaldo = document.getElementById('stat-saldo-aberto');
                    elSaldo.innerText = 'R$ ' + saldoAbertoTotal.toLocaleString('pt-BR', {minimumFractionDigits: 2});
                    if (saldoAbertoTotal >= 0) {
                        elSaldo.className = "text-xs font-bold text-green-400";
                    } else {
                        elSaldo.className = "text-xs font-bold text-red-400";
                    }

                    if (orders.length === 0) {
                        tbody.innerHTML = `<tr><td colspan="12" class="text-center py-8 text-gray-500 italic">Nenhuma posição ou ordem registrada na conta 70960268 até o momento.</td></tr>`;
                        return;
                    }

                    orders.forEach(o => {
                        let dirColor = o.option_type === 'CALL' ? 'text-blue-400' : 'text-purple-400';
                        let dataCompra = new Date(o.created_at).toLocaleString('pt-BR', {day: '2-digit', month: '2-digit', hour: '2-digit', minute:'2-digit'});
                        let dataVencimentoBr = obterDataVencimentoBr(o.option_name, o.created_at);
                        let investedVal = o.invested_capital || (o.lots * o.real_price);
                        let lucroPerc = 0.00;
                        let resultadoBrl = 0.00;
                        let corResultado = resultadoBrl >= 0 ? 'text-green-400' : 'text-red-400';

                        let actionBtn = o.status === 'FILLED' ? 
                            `<button onclick="fecharOperacao(${o.id})" class="bg-red-600 hover:bg-red-700 text-white font-bold px-2.5 py-1 rounded text-xs shadow transition">X</button>` : 
                            `<span class="text-gray-600 text-xs">-</span>`;
                        
                        let row = `<tr class="border-b border-gray-800 hover:bg-gray-800/50 transition">
                            <td class="py-3 px-3 text-gray-400 text-xs">${dataCompra}</td>
                            <td class="py-3 px-3 font-bold text-white">${o.ticker}</td>
                            <td class="py-3 px-3 font-mono text-cyan-400 font-bold">${o.option_name}</td>
                            <td class="py-3 px-3 text-cyan-300 font-mono text-xs font-semibold">${dataVencimentoBr}</td>
                            <td class="py-3 px-3 font-semibold ${dirColor}">${o.option_type}</td>
                            <td class="py-3 px-3 text-yellow-400 font-bold">${o.lots}</td>
                            <td class="py-3 px-3 text-yellow-400 font-bold">R$ ${o.real_price.toFixed(2)}</td>
                            <td class="py-3 px-3 text-yellow-400">R$ ${o.real_price.toFixed(2)}</td>
                            <td class="py-3 px-3 text-green-400 font-mono font-semibold">R$ ${investedVal.toFixed(2)}</td>
                            <td class="py-3 px-3 font-bold ${corResultado}">${lucroPerc.toFixed(2)}%</td>
                            <td class="py-3 px-3 font-bold ${corResultado}">R$ ${resultadoBrl.toFixed(2)}</td>
                            <td class="py-3 px-3 text-center">${actionBtn}</td>
                        </tr>`;
                        tbody.innerHTML += row;
                    });
                } catch (error) {
                    console.error('Erro ao sincronizar dados com o OMS ProfitPro:', error);
                }
            }

            async function fecharOperacao(id) {
                if (!confirm(`Deseja realmente encerrar manualmente a ordem #${id} no ProfitPro (Conta 70960268)?`)) return;
                try {
                    const res = await fetch(`/api/orders/${id}/close`, { method: 'POST' });
                    if (res.ok) fetchOMSData();
                    else alert('Erro ao fechar operação.');
                } catch (e) { alert('Erro de conexão com o servidor OMS.'); }
            }

            async function fecharTodasPosicoes() {
                if (!confirm('Deseja realmente ENCERRAR TODAS AS POSIÇÕES ABERTAS na Conta 70960268?')) return;
                try {
                    const res = await fetch('/api/orders/close-all', { method: 'POST' });
                    if (res.ok) fetchOMSData();
                    else alert('Erro ao encerrar todas as posições.');
                } catch (e) { alert('Erro de conexão com o servidor OMS.'); }
            }

            async function limparBanco() {
                if (!confirm('Deseja realmente apagar todo o histórico do banco de dados local OMS?')) return;
                try {
                    const res = await fetch('/api/orders/reset-db', { method: 'POST' });
                    if (res.ok) fetchOMSData();
                    else alert('Erro ao limpar banco.');
                } catch (e) { alert('Erro de conexão.'); }
            }

            function baixarBackup() {
                window.location.href = '/api/backup/export';
            }

            async function restaurarBackup(event) {
                const file = event.target.files[0];
                if (!file) return;

                if (!confirm(`Deseja restaurar o banco de dados com o arquivo "${file.name}"?`)) {
                    event.target.value = '';
                    return;
                }

                const formData = new FormData();
                formData.append('file', file);

                try {
                    const res = await fetch('/api/backup/restore', {
                        method: 'POST',
                        body: formData
                    });
                    const result = await res.json();
                    if (res.ok) {
                        alert(result.message);
                        fetchOMSData();
                    } else {
                        alert('Erro ao restaurar backup: ' + (result.detail || 'Erro desconhecido'));
                    }
                } catch (e) {
                    alert('Erro de conexão ao enviar arquivo de backup.');
                }
                event.target.value = '';
            }
            
            setInterval(fetchOMSData, 3000);
            window.onload = fetchOMSData;
        </script>
    </head>
    <body class="bg-gray-950 text-gray-100 font-sans antialiased min-h-screen p-6">
        <div class="max-w-7xl mx-auto">
            <!-- Header -->
            <div class="flex justify-between items-center mb-6 border-b border-gray-800 pb-4">
                <div>
                    <h1 class="text-2xl font-black tracking-wider text-blue-500 font-mono">FENIX PRO <span class="text-xs px-2 py-0.5 rounded bg-blue-900/40 text-blue-300 border border-blue-700/50">PROFITPRO OMS (70960268)</span></h1>
                    <p class="text-xs text-gray-400 mt-1">Plataforma de Auditoria, Conexão ProfitPro, Rejeições e Backup Antirresgate do Render</p>
                </div>
                <div class="flex items-center gap-2 flex-wrap justify-end">
                    <button onclick="baixarBackup()" class="bg-emerald-600 hover:bg-emerald-700 text-white font-bold px-3 py-1 rounded text-xs border border-emerald-500 shadow transition flex items-center gap-1">📥 Baixar Backup</button>
                    <label class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold px-3 py-1 rounded text-xs border border-indigo-500 shadow transition cursor-pointer flex items-center gap-1">
                        📤 Restaurar Backup
                        <input type="file" accept=".json" onchange="restaurarBackup(event)" class="hidden">
                    </label>

                    <button onclick="fecharTodasPosicoes()" class="bg-red-600 hover:bg-red-700 text-white font-bold px-3 py-1 rounded text-xs border border-red-500 shadow transition">🚨 Fechar posições</button>
                    <button onclick="limparBanco()" class="bg-gray-800 hover:bg-gray-700 text-gray-300 px-3 py-1 rounded text-xs border border-gray-700 transition">🧹 Limpar</button>
                    <div class="flex items-center gap-2 ml-2">
                        <span class="w-3 h-3 rounded-full bg-green-500 animate-ping"></span>
                        <span class="text-xs text-green-400 font-bold">ONLINE</span>
                    </div>
                </div>
            </div>

            <!-- CARDS DE CONTA E MÉTRICAS -->
            <div class="grid grid-cols-1 md:grid-cols-5 gap-3 mb-6">
                <div class="bg-gray-900 border border-blue-500/40 p-4 rounded-xl shadow-lg relative overflow-hidden">
                    <div class="absolute top-0 right-0 bg-blue-500/10 text-blue-400 text-[10px] px-2 py-1 rounded-bl font-mono">CONTA PROFITPRO</div>
                    <span class="text-xs text-gray-400 block mb-1">IDENTIFICAÇÃO</span>
                    <span id="acc-id" class="text-xl font-black text-white font-mono tracking-wider">Carregando...</span>
                    <span id="acc-broker" class="text-xs text-gray-500 block mt-1">Verificando...</span>
                </div>

                <div class="bg-gray-900 border border-green-500/40 p-4 rounded-xl shadow-lg relative overflow-hidden">
                    <div class="absolute top-0 right-0 bg-green-500/10 text-green-400 text-[10px] px-2 py-1 rounded-bl font-mono">PATRIMÔNIO</div>
                    <span class="text-xs text-gray-400 block mb-1">EQUITY DA CONTA</span>
                    <span id="acc-equity" class="text-xl font-black text-green-400 font-mono tracking-wider">R$ 0,00</span>
                    <span class="text-xs text-gray-500 block mt-1">Investido: <span id="acc-invested" class="text-gray-300 font-semibold">R$ 0,00</span></span>
                </div>

                <div class="bg-gray-900 border border-emerald-500/40 p-4 rounded-xl shadow-lg relative overflow-hidden">
                    <div class="absolute top-0 right-0 bg-emerald-500/10 text-emerald-400 text-[10px] px-2 py-1 rounded-bl font-mono">RESULTADO ABERTO</div>
                    <div class="space-y-0.5 mt-1">
                        <div class="flex justify-between items-center text-xs">
                            <span class="text-gray-400">Lucro Aberto (+):</span>
                            <span id="stat-lucro-aberto" class="font-bold text-green-400">R$ 0,00</span>
                        </div>
                        <div class="flex justify-between items-center text-xs">
                            <span class="text-gray-400">Perda Aberta (-):</span>
                            <span id="stat-perda-aberta" class="font-bold text-red-400">R$ 0,00</span>
                        </div>
                        <div class="flex justify-between items-center text-xs border-t border-gray-800 pt-0.5 mt-0.5">
                            <span class="text-gray-300 font-semibold">Saldo Aberto (=):</span>
                            <span id="stat-saldo-aberto" class="font-bold text-cyan-400">R$ 0,00</span>
                        </div>
                    </div>
                </div>

                <div class="bg-gray-900 border border-purple-500/40 p-4 rounded-xl shadow-lg relative overflow-hidden">
                    <div class="absolute top-0 right-0 bg-purple-500/10 text-purple-400 text-[10px] px-2 py-1 rounded-bl font-mono">CONEXÃO DLL</div>
                    <span class="text-xs text-gray-400 block mb-1">STATUS PROFITPRO</span>
                    <span id="acc-dll" class="text-sm font-bold text-purple-300 mt-1 block">Aguardando...</span>
                    <span class="text-xs text-gray-500 block mt-1">Uptime: <span id="acc-uptime">0h 0m</span></span>
                </div>

                <div class="bg-gray-900 border border-yellow-500/40 p-4 rounded-xl shadow-lg relative overflow-hidden">
                    <div class="absolute top-0 right-0 bg-yellow-500/10 text-yellow-400 text-[10px] px-2 py-1 rounded-bl font-mono">MÉTRICAS</div>
                    <div class="flex justify-between items-center mt-1">
                        <div>
                            <span class="text-[10px] text-gray-400 block">TOTAL</span>
                            <span id="stat-total" class="text-lg font-bold text-white">0</span>
                        </div>
                        <div>
                            <span class="text-[10px] text-gray-400 block">EXECUTADAS</span>
                            <span id="stat-filled" class="text-lg font-bold text-green-400">0</span>
                        </div>
                        <div>
                            <span class="text-[10px] text-gray-400 block">REJEITADAS</span>
                            <span id="stat-rejected" class="text-lg font-bold text-red-400">0</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Tabela de Posições e Ordens -->
            <div class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden shadow-2xl">
                <div class="p-4 border-b border-gray-800 bg-gray-900/50 flex justify-between items-center">
                    <h3 class="font-bold text-sm text-gray-300">📊 POSIÇÕES ABERTAS & EXECUÇÕES RECENTES - PROFITPRO (CONTA 70960268)</h3>
                    <span class="flex items-center gap-2 text-xs text-green-400"><span class="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span> AO VIVO (Auto-refresh 3s)</span>
                </div>
                <div class="overflow-x-auto">
                    <table class="w-full text-left border-collapse text-sm">
                        <thead>
                            <tr class="bg-gray-950/60 text-gray-400 uppercase text-xs tracking-wider border-b border-gray-800">
                                <th class="py-3 px-3">Data</th>
                                <th class="py-3 px-3">Ativo</th>
                                <th class="py-3 px-3">Opção</th>
                                <th class="py-3 px-3">Venc.</th>
                                <th class="py-3 px-3">Tipo</th>
                                <th class="py-3 px-3">Lote</th>
                                <th class="py-3 px-3">P. Compra</th>
                                <th class="py-3 px-3">P. Atual</th>
                                <th class="py-3 px-3">Investido (R$)</th>
                                <th class="py-3 px-3">Lucro (%)</th>
                                <th class="py-3 px-3">Resultado (R$)</th>
                                <th class="py-3 px-3 text-center">Ação</th>
                            </tr>
                        </thead>
                        <tbody id="orders-table">
                            <!-- Injetado via JS -->
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("OMSystem:app", host="0.0.0.0", port=port, reload=False)
