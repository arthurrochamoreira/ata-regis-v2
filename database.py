import sqlite3
import json
from typing import Dict, List, Tuple

DB_PATH = "data.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS atas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero TEXT NOT NULL,
                vigencia TEXT,
                objeto TEXT,
                fornecedor TEXT,
                situacao TEXT,
                valorTotal REAL,
                documentoSei TEXT,
                itens TEXT,
                contatos TEXT
            )
            """
        )
        conn.commit()


def format_currency(value: float) -> str:
    return f"R$ {value:.2f}".replace(".", ",")


def fetch_atas() -> Tuple[Dict[str, List[Dict]], float]:
    """Return atas grouped by situacao and total value."""
    result = {"vigentes": [], "vencidas": [], "aVencer": []}
    total_valor = 0.0
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM atas").fetchall()
        for row in rows:
            ata = dict(row)
            valor = ata.get("valorTotal") or 0.0
            total_valor += valor
            ata["valorTotal"] = format_currency(valor)
            ata["itens"] = json.loads(ata["itens"]) if ata.get("itens") else []
            for item in ata["itens"]:
                vu = item.get("valorUnitario", 0.0)
                subtotal = item.get("subtotal", 0.0)
                item["valorUnitario"] = format_currency(float(vu))
                item["subtotal"] = format_currency(float(subtotal))
            ata["contatos"] = json.loads(ata["contatos"]) if ata.get("contatos") else {"telefone": [], "email": []}
            situacao = (ata.get("situacao") or "").lower()
            if situacao == "vigente":
                result["vigentes"].append(ata)
            elif situacao == "a vencer":
                result["aVencer"].append(ata)
            else:
                result["vencidas"].append(ata)
    return result, total_valor


def delete_ata_db(ata_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM atas WHERE id=?", (ata_id,))
        conn.commit()
