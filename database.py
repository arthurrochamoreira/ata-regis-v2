from __future__ import annotations

"""Database module for managing *Atas de Registro de Preços*.

This module exposes a small ORM layer backed by SQLite and SQLAlchemy 2.x.
It is designed to be plug-and-play with the Flet desktop UI.  All values of
money are stored in **centavos** (integers) and only formatted on the edges of
application.  It also provides a full text search index (FTS5) and several
helpers for formatting and parsing Brazilian currency strings.
"""

# To migrate to another backend (e.g. PostgreSQL) adjust :func:`get_engine`
# and port FTS triggers to the target dialect.

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

from sqlalchemy import (
    CheckConstraint,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    event,
    func,
    select,
    text,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import (
    Mapped,
    Session,
    declarative_base,
    mapped_column,
    relationship,
    scoped_session,
    sessionmaker, selectinload,
)

# ================================================================
# ORM MODELS
# ================================================================

Base = declarative_base()


class Config(Base):
    __tablename__ = "config"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String, nullable=False)


class Fornecedor(Base):
    __tablename__ = "fornecedor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    cnpj: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    observacoes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    contatos: Mapped[List["FornecedorContato"]] = relationship(
        back_populates="fornecedor", cascade="all, delete-orphan"
    )
    atas: Mapped[List["Ata"]] = relationship(back_populates="fornecedor")


class FornecedorContato(Base):
    __tablename__ = "fornecedor_contato"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fornecedor_id: Mapped[int] = mapped_column(
        ForeignKey("fornecedor.id", ondelete="CASCADE"), nullable=False
    )
    tipo: Mapped[str] = mapped_column(String, nullable=False)
    valor: Mapped[str] = mapped_column(String, nullable=False)
    rotulo: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    fornecedor: Mapped[Fornecedor] = relationship(back_populates="contatos")


class Ata(Base):
    __tablename__ = "ata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    numero: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    sei: Mapped[Optional[str]] = mapped_column(String, unique=True)
    objeto: Mapped[str] = mapped_column(Text, nullable=False)
    fornecedor_id: Mapped[int] = mapped_column(
        ForeignKey("fornecedor.id"), nullable=False, index=True
    )
    data_inicio: Mapped[date] = mapped_column(Date, nullable=False)
    data_fim: Mapped[date] = mapped_column(Date, nullable=False)
    valor_total_centavos: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    fornecedor: Mapped[Fornecedor] = relationship(back_populates="atas")
    itens: Mapped[List["AtaItem"]] = relationship(
        back_populates="ata", cascade="all, delete-orphan"
    )
    contatos: Mapped[List["AtaContato"]] = relationship(
        back_populates="ata", cascade="all, delete-orphan"
    )
    anexos: Mapped[List["Anexo"]] = relationship(
        back_populates="ata", cascade="all, delete-orphan"
    )


class AtaItem(Base):
    __tablename__ = "ata_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ata_id: Mapped[int] = mapped_column(
        ForeignKey("ata.id", ondelete="CASCADE"), nullable=False, index=True
    )
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    quantidade: Mapped[int] = mapped_column(Integer, nullable=False)
    valor_unit_centavos: Mapped[int] = mapped_column(Integer, nullable=False)
    subtotal_centavos: Mapped[int] = mapped_column(
        Integer, Computed("quantidade * valor_unit_centavos", persisted=True)
    )

    ata: Mapped[Ata] = relationship(back_populates="itens")

    __table_args__ = (
        CheckConstraint("quantidade > 0"),
        CheckConstraint("valor_unit_centavos >= 0"),
    )


class AtaContato(Base):
    __tablename__ = "ata_contato"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ata_id: Mapped[int] = mapped_column(
        ForeignKey("ata.id", ondelete="CASCADE"), nullable=False
    )
    tipo: Mapped[str] = mapped_column(String, nullable=False)
    valor: Mapped[str] = mapped_column(String, nullable=False)
    rotulo: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    ata: Mapped[Ata] = relationship(back_populates="contatos")


class Anexo(Base):
    __tablename__ = "anexo"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ata_id: Mapped[int] = mapped_column(
        ForeignKey("ata.id", ondelete="CASCADE"), nullable=False, index=True
    )
    tipo: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    nome: Mapped[str] = mapped_column(String, nullable=False)
    caminho: Mapped[str] = mapped_column(String, nullable=False)
    hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    ata: Mapped[Ata] = relationship(back_populates="anexos")


# ================================================================
# ENGINE / SESSION MANAGEMENT
# ================================================================

engine = None
SessionLocal: scoped_session
fts_enabled = True


def get_engine(db_path: str, echo: bool = False):
    eng = create_engine(
        f"sqlite+pysqlite:///{db_path}", echo=echo, future=True
    )

    @event.listens_for(eng, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):  # pragma: no cover
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return eng


# ================================================================
# UTILITIES
# ================================================================


def format_currency(valor_centavos: Optional[int]) -> str:
    """Format integer centavos to Brazilian currency string.

    >>> format_currency(69010)
    'R$ 690,10'
    >>> format_currency(None)
    'R$ 0,00'
    """

    valor = 0 if valor_centavos is None else int(valor_centavos)
    reais, cent = divmod(valor, 100)
    reais_str = f"{reais:,}".replace(",", ".")
    return f"R$ {reais_str},{cent:02d}"


def parse_currency(texto: str) -> int:
    """Parse Brazilian currency string into integer centavos.

    >>> parse_currency('R$ 1.234,56')
    123456
    >>> parse_currency('0')
    0
    """

    clean = (
        texto.replace("R$", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )
    if not clean:
        return 0
    try:
        dec = Decimal(clean)
    except InvalidOperation as exc:  # pragma: no cover - validation
        raise ValueError(f"Valor monetário inválido: {texto!r}") from exc
    return int(dec * 100)


def iso_to_br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def calcular_situacao(data_fim: date, dias_alerta: int) -> str:
    hoje = date.today()
    if data_fim < hoje:
        return "vencida"
    delta = (data_fim - hoje).days
    if 0 <= delta <= dias_alerta:
        return "a vencer"
    return "vigente"


def pretty_situacao(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split())


# ================================================================
# DATABASE INITIALIZATION
# ================================================================


def init_db(db_path: str = "ata_regis.db") -> None:
    """Initialise the SQLite database and create all structures."""

    global engine, SessionLocal, fts_enabled

    engine = get_engine(db_path)
    SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False))

    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        # Indices
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fornecedor_nome ON fornecedor(nome)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ata_data_fim ON ata(data_fim)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ata_fornecedor ON ata(fornecedor_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ata_item_ata ON ata_item(ata_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_anexo_ata ON anexo(ata_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fornecedor_contato_forn ON fornecedor_contato(fornecedor_id)"))

        # View for situacao
        conn.execute(text("DROP VIEW IF EXISTS v_ata_situacao"))
        conn.execute(
            text(
                """
                CREATE VIEW v_ata_situacao AS
                SELECT a.*,
                CASE
                    WHEN date(a.data_fim) < date('now') THEN 'vencida'
                    WHEN julianday(a.data_fim) - julianday('now') <= COALESCE((SELECT CAST(value AS INTEGER) FROM config WHERE key='dias_alerta_vencimento'),60)
                         AND julianday(a.data_fim) - julianday('now') >= 0 THEN 'a vencer'
                    ELSE 'vigente'
                END AS situacao
                FROM ata a
                """
            )
        )

        # Aggregation triggers for valor_total_centavos
        conn.execute(text("DROP TRIGGER IF EXISTS ata_item_ai"))
        conn.execute(text("DROP TRIGGER IF EXISTS ata_item_au"))
        conn.execute(text("DROP TRIGGER IF EXISTS ata_item_ad"))
        conn.execute(
            text(
                """
                CREATE TRIGGER ata_item_ai AFTER INSERT ON ata_item BEGIN
                    UPDATE ata SET valor_total_centavos = (
                        SELECT COALESCE(SUM(subtotal_centavos),0) FROM ata_item WHERE ata_id=NEW.ata_id
                    ) WHERE id=NEW.ata_id;
                END;
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TRIGGER ata_item_au AFTER UPDATE ON ata_item BEGIN
                    UPDATE ata SET valor_total_centavos = (
                        SELECT COALESCE(SUM(subtotal_centavos),0) FROM ata_item WHERE ata_id=NEW.ata_id
                    ) WHERE id=NEW.ata_id;
                END;
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TRIGGER ata_item_ad AFTER DELETE ON ata_item BEGIN
                    UPDATE ata SET valor_total_centavos = (
                        SELECT COALESCE(SUM(subtotal_centavos),0) FROM ata_item WHERE ata_id=OLD.ata_id
                    ) WHERE id=OLD.ata_id;
                END;
                """
            )
        )

        # Try to create FTS structures
        try:
            conn.execute(
                text(
                    """
                    CREATE VIRTUAL TABLE IF NOT EXISTS ata_fts USING fts5(
                        numero, objeto, fornecedor_nome, itens_text
                    )
                    """
                )
            )
            conn.execute(text("DROP TRIGGER IF EXISTS ata_ai_fts"))
            conn.execute(text("DROP TRIGGER IF EXISTS ata_au_fts"))
            conn.execute(text("DROP TRIGGER IF EXISTS ata_ad_fts"))
            conn.execute(text("DROP TRIGGER IF EXISTS ata_item_ai_fts"))
            conn.execute(text("DROP TRIGGER IF EXISTS ata_item_au_fts"))
            conn.execute(text("DROP TRIGGER IF EXISTS ata_item_ad_fts"))
            conn.execute(
                text(
                    """
                    CREATE TRIGGER ata_ai_fts AFTER INSERT ON ata BEGIN
                        INSERT INTO ata_fts(rowid, numero, objeto, fornecedor_nome, itens_text)
                        VALUES (
                            NEW.id,
                            NEW.numero,
                            NEW.objeto,
                            (SELECT nome FROM fornecedor WHERE id=NEW.fornecedor_id),
                            (SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.id)
                        );
                    END;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TRIGGER ata_au_fts AFTER UPDATE ON ata BEGIN
                        UPDATE ata_fts SET
                            numero=NEW.numero,
                            objeto=NEW.objeto,
                            fornecedor_nome=(SELECT nome FROM fornecedor WHERE id=NEW.fornecedor_id),
                            itens_text=(SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.id)
                        WHERE rowid=NEW.id;
                    END;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TRIGGER ata_ad_fts AFTER DELETE ON ata BEGIN
                        DELETE FROM ata_fts WHERE rowid=OLD.id;
                    END;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TRIGGER ata_item_ai_fts AFTER INSERT ON ata_item BEGIN
                        UPDATE ata_fts SET itens_text=(
                            SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.ata_id
                        ) WHERE rowid=NEW.ata_id;
                    END;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TRIGGER ata_item_au_fts AFTER UPDATE ON ata_item BEGIN
                        UPDATE ata_fts SET itens_text=(
                            SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=NEW.ata_id
                        ) WHERE rowid=NEW.ata_id;
                    END;
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TRIGGER ata_item_ad_fts AFTER DELETE ON ata_item BEGIN
                        UPDATE ata_fts SET itens_text=(
                            SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=OLD.ata_id
                        ) WHERE rowid=OLD.ata_id;
                    END;
                    """
                )
            )
            fts_enabled = True
        except OperationalError:
            fts_enabled = False

        # Ensure default config
        conn.execute(
            text(
                "INSERT OR IGNORE INTO config(key, value) VALUES ('dias_alerta_vencimento', '60')"
            )
        )

    # Seed example data if database empty
    with SessionLocal() as session:
        if session.scalar(select(func.count(Fornecedor.id))) == 0:
            _seed(session)
            session.commit()


def _seed(session: Session) -> None:
    """Populate database with a very small sample dataset."""

    forn = Fornecedor(nome="JIIJ Comércio")
    session.add(forn)
    session.flush()
    session.add_all(
        [
            FornecedorContato(fornecedor_id=forn.id, tipo="telefone", valor="(61) 99999-9999"),
            FornecedorContato(fornecedor_id=forn.id, tipo="email", valor="contato@jiij.com"),
        ]
    )

    hoje = date.today()
    dados_atas = [
        # Vigente (> 60 dias)
        {
            "numero": "1000/2024",
            "objeto": "Material de escritório",
            "data_inicio": hoje.replace(year=hoje.year - 1),
            "data_fim": hoje.replace(year=hoje.year + 1),
        },
        # A vencer (<= 60 dias)
        {
            "numero": "1001/2024",
            "objeto": "Serviços de limpeza",
            "data_inicio": hoje.replace(year=hoje.year - 1),
            "data_fim": hoje + timedelta(days=30),
        },
        # Vencida
        {
            "numero": "1002/2024",
            "objeto": "Equipamentos de TI",
            "data_inicio": hoje.replace(year=hoje.year - 2),
            "data_fim": hoje - timedelta(days=10),
        },
    ]
    for dados in dados_atas:
        ata = Ata(
            numero=dados["numero"],
            objeto=dados["objeto"],
            fornecedor_id=forn.id,
            data_inicio=dados["data_inicio"],
            data_fim=dados["data_fim"],
        )
        session.add(ata)
        session.flush()
        session.add(
            AtaItem(
                ata_id=ata.id,
                descricao="Item exemplo",
                quantidade=1,
                valor_unit_centavos=1000,
            )
        )


# ================================================================
# CONFIG PARAMS
# ================================================================


def set_param(name: str, value: str) -> None:
    with SessionLocal() as session:
        session.merge(Config(key=name, value=value))
        session.commit()


def get_param(name: str, default: Optional[str] = None) -> str:
    with SessionLocal() as session:
        cfg = session.get(Config, name)
        return cfg.value if cfg else default


# ================================================================
# CRUD HELPERS
# ================================================================


def create_fornecedor(
    nome: str, cnpj: Optional[str] = None, observacoes: Optional[str] = None
) -> int:
    with SessionLocal() as session:
        forn = Fornecedor(nome=nome, cnpj=cnpj, observacoes=observacoes)
        session.add(forn)
        session.commit()
        return forn.id


def get_or_create_fornecedor(nome: str) -> int:
    """Return fornecedor id, creating it if necessary.

    This is a convenience helper for the UI layer where users may type the
    supplier name directly. If a fornecedor with the given ``nome`` already
    exists it is reused; otherwise a new row is inserted.
    """
    with SessionLocal() as session:
        forn = session.query(Fornecedor).filter_by(nome=nome).one_or_none()
        if forn:
            return forn.id
        forn = Fornecedor(nome=nome)
        session.add(forn)
        session.commit()
        return forn.id


def add_fornecedor_contato(
    fornecedor_id: int, tipo: str, valor: str, rotulo: Optional[str] = None
) -> int:
    with SessionLocal() as session:
        contato = FornecedorContato(
            fornecedor_id=fornecedor_id, tipo=tipo, valor=valor, rotulo=rotulo
        )
        session.add(contato)
        session.commit()
        return contato.id


def create_ata(dto: dict) -> int:
    itens = dto.get("itens", [])
    contatos = dto.get("contatos", [])
    ata = Ata(
        numero=dto["numero"],
        sei=dto.get("sei"),
        objeto=dto["objeto"],
        fornecedor_id=dto["fornecedor_id"],
        data_inicio=date.fromisoformat(dto["data_inicio"]),
        data_fim=date.fromisoformat(dto["data_fim"]),
    )
    with SessionLocal() as session:
        session.add(ata)
        session.flush()
        for item in itens:
            session.add(
                AtaItem(
                    ata_id=ata.id,
                    descricao=item["descricao"],
                    quantidade=item["quantidade"],
                    valor_unit_centavos=item["valor_unit_centavos"],
                )
            )
        for c in contatos:
            session.add(
                AtaContato(
                    ata_id=ata.id,
                    tipo=c["tipo"],
                    valor=c["valor"],
                    rotulo=c.get("rotulo"),
                )
            )
        session.commit()
        return ata.id


def update_ata(ata_id: int, dto: dict) -> None:
    with SessionLocal() as session:
        ata = session.get(Ata, ata_id)
        if not ata:
            raise ValueError("Ata não encontrada")
        for field in ("numero", "sei", "objeto", "fornecedor_id"):
            if field in dto:
                setattr(ata, field, dto[field])
        if "data_inicio" in dto:
            ata.data_inicio = date.fromisoformat(dto["data_inicio"])
        if "data_fim" in dto:
            ata.data_fim = date.fromisoformat(dto["data_fim"])
        if "itens" in dto:
            session.query(AtaItem).filter_by(ata_id=ata_id).delete()
            for item in dto["itens"]:
                session.add(
                    AtaItem(
                        ata_id=ata_id,
                        descricao=item["descricao"],
                        quantidade=item["quantidade"],
                        valor_unit_centavos=item["valor_unit_centavos"],
                    )
                )
        if "contatos" in dto:
            session.query(AtaContato).filter_by(ata_id=ata_id).delete()
            for c in dto["contatos"]:
                session.add(
                    AtaContato(
                        ata_id=ata_id,
                        tipo=c["tipo"],
                        valor=c["valor"],
                        rotulo=c.get("rotulo"),
                    )
                )
        session.commit()


def delete_ata_db(ata_id: int) -> None:
    with SessionLocal() as session:
        ata = session.get(Ata, ata_id)
        if ata:
            session.delete(ata)
            session.commit()


# ================================================================
# QUERY HELPERS
# ================================================================


def _ata_to_dict(ata: Ata, situacao: Optional[str] = None) -> dict:
    dias_alerta = int(get_param("dias_alerta_vencimento", "60"))
    sit = situacao or calcular_situacao(ata.data_fim, dias_alerta)
    result = {
        "id": ata.id,
        "numero": ata.numero,
        "vigencia": iso_to_br(ata.data_fim),
        "objeto": ata.objeto,
        "fornecedor": ata.fornecedor.nome,
        "situacao": pretty_situacao(sit),
        "valorTotal": format_currency(ata.valor_total_centavos),
        "documentoSei": ata.sei or "",
        "itens": [
            {
                "descricao": i.descricao,
                "quantidade": i.quantidade,
                "valorUnitario": format_currency(i.valor_unit_centavos),
                "subtotal": format_currency(i.subtotal_centavos),
            }
            for i in ata.itens
        ],
        "contatos": {"telefone": [], "email": []},
    }
    for c in ata.contatos:
        result["contatos"][c.tipo].append(c.valor)
    return result


def get_ata_by_id(ata_id: int) -> dict:
    with SessionLocal() as session:
        ata = (
            session.query(Ata)
            .filter(Ata.id == ata_id)
            .options(
                selectinload(Ata.fornecedor),
                selectinload(Ata.itens),
                selectinload(Ata.contatos)
            )
            .one_or_none()
        )
        if not ata:
            raise ValueError("Ata não encontrada")
        return _ata_to_dict(ata)


def fetch_atas(
    filters: Optional[Dict[str, bool]] = None,
    search: Optional[str] = None,
    order: str = "data_fim_asc",
) -> dict:
    filters = filters or {}
    res = {"vigentes": [], "vencidas": [], "aVencer": []}

    order_map = {
        "data_fim_asc": "v.data_fim ASC",
        "data_fim_desc": "v.data_fim DESC",
        "numero_asc": "v.numero ASC",
        "numero_desc": "v.numero DESC",
    }
    order_clause = order_map.get(order, "v.data_fim ASC")

    base_sql = "SELECT v.id, v.situacao FROM v_ata_situacao v JOIN fornecedor f ON f.id=v.fornecedor_id"
    where_clauses = []
    params = {}
    if search:
        if fts_enabled:
            base_sql += " JOIN ata_fts ft ON ft.rowid = v.id"
            where_clauses.append("ft MATCH :q")
            params["q"] = search
        else:
            where_clauses.append(
                "(v.objeto LIKE :like OR v.numero LIKE :like OR f.nome LIKE :like OR EXISTS(SELECT 1 FROM ata_item ai WHERE ai.ata_id=v.id AND ai.descricao LIKE :like))"
            )
            params["like"] = f"%{search}%"
    conds = []
    if filters.get("vigente"):
        conds.append("v.situacao='vigente'")
    if filters.get("vencida"):
        conds.append("v.situacao='vencida'")
    if filters.get("a_vencer"):
        conds.append("v.situacao='a vencer'")
    if conds:
        where_clauses.append("(" + " OR ".join(conds) + ")")
    if where_clauses:
        base_sql += " WHERE " + " AND ".join(where_clauses)
    base_sql += f" ORDER BY {order_clause}"

    with engine.connect() as conn:
        rows = conn.execute(text(base_sql), params).mappings().all()

    for row in rows:
        ata_dict = get_ata_by_id(row["id"])
        key = {
            "vigente": "vigentes",
            "vencida": "vencidas",
            "a vencer": "aVencer",
        }[row["situacao"]]
        res[key].append(ata_dict)

    if any(filters.values()):
        # When filters applied, remove empty lists for unchecked ones
        if not filters.get("vigente"):
            res["vigentes"] = []
        if not filters.get("vencida"):
            res["vencidas"] = []
        if not filters.get("a_vencer"):
            res["aVencer"] = []
    return res


def rebuild_fts_index() -> None:
    if not fts_enabled:
        return
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ata_fts"))
        conn.execute(
            text(
                """
                INSERT INTO ata_fts(rowid, numero, objeto, fornecedor_nome, itens_text)
                SELECT a.id, a.numero, a.objeto, f.nome,
                       COALESCE((SELECT GROUP_CONCAT(descricao,' ') FROM ata_item WHERE ata_id=a.id),'')
                FROM ata a JOIN fornecedor f ON f.id=a.fornecedor_id
                """
            )
        )



