-- SQLite schema for Ata de Registro de Preços
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fornecedor (
    id          INTEGER PRIMARY KEY,
    nome        TEXT NOT NULL,
    cnpj        TEXT,
    observacoes TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fornecedor_contato (
    id            INTEGER PRIMARY KEY,
    fornecedor_id INTEGER NOT NULL REFERENCES fornecedor(id) ON DELETE CASCADE,
    tipo          TEXT NOT NULL,
    valor         TEXT NOT NULL,
    rotulo        TEXT
);

CREATE TABLE IF NOT EXISTS ata (
    id                   INTEGER PRIMARY KEY,
    numero               TEXT NOT NULL UNIQUE,
    sei                  TEXT UNIQUE,
    objeto               TEXT NOT NULL,
    fornecedor_id        INTEGER NOT NULL REFERENCES fornecedor(id),
    data_inicio          DATE NOT NULL,
    data_fim             DATE NOT NULL,
    valor_total_centavos INTEGER NOT NULL DEFAULT 0,
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ata_item (
    id                 INTEGER PRIMARY KEY,
    ata_id             INTEGER REFERENCES ata(id) ON DELETE CASCADE,
    descricao          TEXT NOT NULL,
    quantidade         INTEGER NOT NULL CHECK (quantidade > 0),
    valor_unit_centavos INTEGER NOT NULL CHECK (valor_unit_centavos >= 0),
    subtotal_centavos  INTEGER GENERATED ALWAYS AS (quantidade*valor_unit_centavos) STORED
);

CREATE TABLE IF NOT EXISTS ata_contato (
    id      INTEGER PRIMARY KEY,
    ata_id  INTEGER REFERENCES ata(id) ON DELETE CASCADE,
    tipo    TEXT NOT NULL,
    valor   TEXT NOT NULL,
    rotulo  TEXT
);

CREATE TABLE IF NOT EXISTS anexo (
    id       INTEGER PRIMARY KEY,
    ata_id   INTEGER REFERENCES ata(id) ON DELETE CASCADE,
    tipo     TEXT,
    nome     TEXT NOT NULL,
    caminho  TEXT NOT NULL,
    hash     TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_fornecedor_nome ON fornecedor(nome);
CREATE INDEX IF NOT EXISTS idx_ata_data_fim ON ata(data_fim);
CREATE INDEX IF NOT EXISTS idx_ata_fornecedor ON ata(fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_ata_item_ata ON ata_item(ata_id);
CREATE INDEX IF NOT EXISTS idx_anexo_ata ON anexo(ata_id);
CREATE INDEX IF NOT EXISTS idx_fornecedor_contato_forn ON fornecedor_contato(fornecedor_id);

-- View with derived situacao
CREATE VIEW v_ata_situacao AS
SELECT a.*,
       CASE
         WHEN date(a.data_fim) < date('now') THEN 'vencida'
         WHEN julianday(a.data_fim) - julianday('now') <= COALESCE((SELECT CAST(value AS INTEGER) FROM config WHERE key='dias_alerta_vencimento'),60)
              AND julianday(a.data_fim) - julianday('now') >= 0 THEN 'a vencer'
         ELSE 'vigente'
       END AS situacao
FROM ata a;

-- Triggers aggregating valor_total_centavos
CREATE TRIGGER ata_item_ai AFTER INSERT ON ata_item BEGIN
    UPDATE ata SET valor_total_centavos=(
        SELECT COALESCE(SUM(subtotal_centavos),0) FROM ata_item WHERE ata_id=NEW.ata_id
    ) WHERE id=NEW.ata_id;
END;
CREATE TRIGGER ata_item_au AFTER UPDATE ON ata_item BEGIN
    UPDATE ata SET valor_total_centavos=(
        SELECT COALESCE(SUM(subtotal_centavos),0) FROM ata_item WHERE ata_id=NEW.ata_id
    ) WHERE id=NEW.ata_id;
END;
CREATE TRIGGER ata_item_ad AFTER DELETE ON ata_item BEGIN
    UPDATE ata SET valor_total_centavos=(
        SELECT COALESCE(SUM(subtotal_centavos),0) FROM ata_item WHERE ata_id=OLD.ata_id
    ) WHERE id=OLD.ata_id;
END;

-- FTS5 virtual table and triggers
CREATE VIRTUAL TABLE ata_fts USING fts5(numero, objeto, fornecedor_nome, itens_text);
CREATE TRIGGER ata_ai_fts AFTER INSERT ON ata BEGIN
    INSERT INTO ata_fts(rowid, numero, objeto, fornecedor_nome, itens_text)
    VALUES (NEW.id, NEW.numero, NEW.objeto,
            (SELECT nome FROM fornecedor WHERE id=NEW.fornecedor_id),
            (SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.id));
END;
CREATE TRIGGER ata_au_fts AFTER UPDATE ON ata BEGIN
    UPDATE ata_fts SET
        numero=NEW.numero,
        objeto=NEW.objeto,
        fornecedor_nome=(SELECT nome FROM fornecedor WHERE id=NEW.fornecedor_id),
        itens_text=(SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.id)
    WHERE rowid=NEW.id;
END;
CREATE TRIGGER ata_ad_fts AFTER DELETE ON ata BEGIN
    DELETE FROM ata_fts WHERE rowid=OLD.id;
END;
CREATE TRIGGER ata_item_ai_fts AFTER INSERT ON ata_item BEGIN
    UPDATE ata_fts SET itens_text=(SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.ata_id)
    WHERE rowid=NEW.ata_id;
END;
CREATE TRIGGER ata_item_au_fts AFTER UPDATE ON ata_item BEGIN
    UPDATE ata_fts SET itens_text=(SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.ata_id)
    WHERE rowid=NEW.ata_id;
END;
CREATE TRIGGER ata_item_ad_fts AFTER DELETE ON ata_item BEGIN
    UPDATE ata_fts SET itens_text=(SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=OLD.ata_id)
    WHERE rowid=OLD.ata_id;
END;

INSERT OR IGNORE INTO config(key,value) VALUES ('dias_alerta_vencimento','60');
