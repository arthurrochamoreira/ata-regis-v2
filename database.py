import sqlite3
import json
from pathlib import Path

DB_PATH = Path('data.db')


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.execute(
        '''CREATE TABLE IF NOT EXISTS atas (
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
           )'''
    )
    conn.commit()
    conn.close()


def insert_ata(data: dict):
    conn = get_connection()
    conn.execute(
        '''INSERT INTO atas (numero, vigencia, objeto, fornecedor, situacao, valorTotal, documentoSei, itens, contatos)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            data.get('numero'),
            data.get('vigencia'),
            data.get('objeto'),
            data.get('fornecedor'),
            data.get('situacao'),
            data.get('valorTotal'),
            data.get('documentoSei'),
            json.dumps(data.get('itens', [])),
            json.dumps(data.get('contatos', {})),
        )
    )
    conn.commit()
    conn.close()


def update_ata(ata_id: int, data: dict):
    conn = get_connection()
    conn.execute(
        '''UPDATE atas SET numero=?, vigencia=?, objeto=?, fornecedor=?, situacao=?, valorTotal=?, documentoSei=?, itens=?, contatos=?
           WHERE id=?''',
        (
            data.get('numero'),
            data.get('vigencia'),
            data.get('objeto'),
            data.get('fornecedor'),
            data.get('situacao'),
            data.get('valorTotal'),
            data.get('documentoSei'),
            json.dumps(data.get('itens', [])),
            json.dumps(data.get('contatos', {})),
            ata_id,
        )
    )
    conn.commit()
    conn.close()


def _format_currency(value: float) -> str:
    return f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def fetch_atas() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM atas')
    rows = cur.fetchall()
    conn.close()
    atas = []
    for r in rows:
        ata = dict(r)
        ata['itens'] = json.loads(ata.get('itens') or '[]')
        ata['contatos'] = json.loads(ata.get('contatos') or '{}')
        ata['valorTotal'] = _format_currency(ata.get('valorTotal') or 0)
        for it in ata['itens']:
            vu = it.get('valorUnitario') or 0
            it['valorUnitario'] = _format_currency(vu)
            sub = it.get('subtotal') or (vu * it.get('quantidade', 0))
            it['subtotal'] = _format_currency(sub)
        atas.append(ata)
    return atas


def fetch_atas_grouped() -> dict:
    result = {'vigentes': [], 'vencidas': [], 'aVencer': []}
    for ata in fetch_atas():
        situacao = ata.get('situacao', '').lower()
        if situacao == 'vigente':
            result['vigentes'].append(ata)
        elif situacao == 'vencida':
            result['vencidas'].append(ata)
        else:
            result['aVencer'].append(ata)
    return result


def get_dashboard() -> dict:
    atas = fetch_atas()
    total = len(atas)
    vigentes = len([a for a in atas if a.get('situacao', '').lower() == 'vigente'])
    a_vencer = len([a for a in atas if a.get('situacao', '').lower() == 'a vencer'])
    valor_total = 0.0
    for a in atas:
        try:
            valor_total += float(str(a.get('valorTotal')).replace('R$ ', '').replace('.', '').replace(',', '.'))
        except Exception:
            pass
    return {
        'total': total,
        'valorTotal': _format_currency(valor_total),
        'vigentes': vigentes,
        'aVencer': a_vencer,
    }
