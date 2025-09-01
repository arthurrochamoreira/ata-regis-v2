import math
import flet as ft
import re
from datetime import date, datetime
from typing import Optional, List

import sqlite3

import database as db

# ==============================
# DESIGN TOKENS
# ==============================

TOKENS = {
    "colors": {
        "base": {
            "black":  ft.Colors.BLACK, # Cor base para opacidades (sombras/divisores)
        },
        "bg": {
            "app": {
                "light":  ft.Colors.GREY_100,      # Fundo global da aplicação (modo claro)
                "dark": "#0f172a",          # Fundo global da aplicação (modo escuro) - slate-900
            },
            "surface": {
                "light":  ft.Colors.WHITE,         # Fundo de cards e superfícies (modo claro)
                "dark":  ft.Colors.with_opacity (0.5,"#1e293b"),       # Fundo de cards e superfícies (modo escuro) - slate-800
                "muted":  ft.Colors.with_opacity(0.2,  ft.Colors.BLACK), # Fundo de cabeçalhos de tabela
            },
            "input": {
                "default":  ft.Colors.with_opacity(0.04,  ft.Colors.BLACK), # Fundo do campo de busca
            },
        },
        "text": {
            "primary": {
                "light":  ft.Colors.GREY_900,      # Cor de texto principal (modo claro)
                "dark": "#f1f5f9",          # Cor de texto principal (modo escuro) - slate-100
            },
            "muted": {
                "light":  ft.Colors.GREY_800,      # Cor de texto secundário/silenciado (modo claro)
                "dark": "#cbd5e1",          # Cor de texto secundário/silenciado (modo escuro) - slate-300
            },
            "inverse":  ft.Colors.WHITE,          # Cor de texto sobre fundos coloridos/escuros
        },
        "border": {
            "default": {
                "light":  ft.Colors.GREY_300,      # Cor de borda padrão (modo claro)
                "dark": "#334155",          # Cor de borda padrão (modo escuro) - slate-700
            }
        },
        "divider": {
            "default": {
                "light":  ft.Colors.with_opacity(0.08,  ft.Colors.BLACK), # Cor de divisores (modo claro)
                "dark":  "#334155",  # Cor de divisores (modo escuro) - slate-700
            },
        },
        "brand": {
            "primary": {
                "bg": "#4f46e5",          # Cor de fundo para botões de ação primária - indigo-600
            }
        },
        "component": {
            "sidebar": {
                "bg": {
                    "light":  ft.Colors.WHITE,   # Fundo da sidebar (modo claro)
                    "dark": "#1e293b",    # Fundo da sidebar (modo escuro) - slate-800
                },
                "active": {
                    "bg": {
                        "light": "#EDE9FE",      # Fundo do item ativo da sidebar (modo claro)
                        "dark": "#312e81",       # Fundo do item ativo da sidebar (modo escuro) - indigo-900
                    },
                    "bar": {
                        "light": "#8B5CF6",      # Barra lateral do item ativo (modo claro)
                        "dark": "#A78BFA",       # Barra lateral do item ativo (modo escuro)
                    },
                    "text": {
                        "light": "#6D28D9",      # Texto/ícone do item ativo (modo claro)
                        "dark":  ft.Colors.WHITE,       # Texto/ícone do item ativo (modo escuro)
                    },
                },
                "icon": {
                    "logo": "#EDE9FE", # Cor do ícone do logo (diamond)
                    "menu": {              # Cor do ícone do menu (hambúrguer)
                        "light":  ft.Colors.GREY_800,
                        "dark": "#EDE9FE"  # slate-400
                    },
                    "inactive": {              # Cor dos ícones de navegação inativos
                        "light":  ft.Colors.GREY_800,
                        "dark": "#EDE9FE"  # slate-400
                    },
                    "theme": {              # Cor do ícone de alternar tema
                        "light":  ft.Colors.GREY_600,
                        "dark": "#EDE9FE"  # slate-400
                    }
                },
            },
        },
        "semantic": {
            "success": {
                "bg":  ft.Colors.GREEN_700,      # Fundo para snackbar de sucesso
            },
            "error": {
                "bg":  ft.Colors.RED_700,        # Fundo para snackbar de erro
                "bg_strong":  ft.Colors.RED,       # Fundo para botões de exclusão
                "icon":  ft.Colors.RED_400,        # Ícone de exclusão
            },
            "warning": {
                "bg": {
                    "light":  ft.Colors.AMBER_100, # Fundo do card de aviso (modo claro)
                    "dark":  ft.Colors.AMBER_900,  # Fundo do card de aviso (modo escuro)
                },
                "border":  ft.Colors.AMBER,        # Borda e ícone do card de aviso
            },
        },
        "status": {
            "vigente": { # Verde
                "bg": {"light":  ft.Colors.GREEN_100, "dark":  ft.Colors.with_opacity(0.1, "#4ade80")},
                "text": {"light":  ft.Colors.GREEN_800, "dark": "#86efac"}, # green-300
            },
            "a_vencer": { # Âmbar
                "bg": {"light":  ft.Colors.AMBER_50, "dark":  ft.Colors.with_opacity(0.1, "#facc15")},
                "text": {"light":  ft.Colors.AMBER_900, "dark": "#facc15"}, # yellow-400
            },
            "vencida": { # Vermelho
                "bg": {"light":  ft.Colors.RED_100, "dark":  ft.Colors.with_opacity(0.1, "#f87171")},
                "text": {"light":  ft.Colors.RED_800, "dark": "#fca5a5"}, # red-300
            },
        },
        "chart": {
            "success": "#10B981",              # Cor verde para gráficos (vigentes)
            "warning": "#FF6F00",              # Cor âmbar para gráficos (a vencer)
            "error": "#EF4444",              # Cor vermelha para gráficos (vencidas)
            "default": {
                "light": "#212121",              # Cor padrão de barras de gráfico (modo claro)
                "dark": "#334155",               # Cor padrão de barras de gráfico (modo escuro) - slate-700
            },
        },
        "shadow": {
            "strong":  ft.Colors.with_opacity(0.15,  ft.Colors.BLACK),
            "default":  ft.Colors.with_opacity(0.12,  ft.Colors.BLACK),
            "soft":  ft.Colors.with_opacity(0.10,  ft.Colors.BLACK),
            "faint":  ft.Colors.with_opacity(0.08,  ft.Colors.BLACK),
        },
    }
}


# Constantes de layout
W_COLLAPSED = 80
W_EXPANDED = 256
P_ROOT = 16
P_ITEM = 12
ICON_SIZE = 24
RADIUS_ASIDE = 24
R_ITEM = 12
BAR_W = 6
ANIM = ft.Animation(300, "easeInOut")

# DRY Principle: Define filter keys once.
FILTER_KEYS = ('vigente', 'vencida', 'a_vencer')

# Alturas/paddings padronizados para PÍLULA
PILL = {
    "sm": {"h": 36, "px": 12, "font": 12},
    "md": {"h": 44, "px": 16, "font": 14},
    "lg": {"h": 52, "px": 20, "font": 16},
}
DEFAULT_PILL_SIZE = "md"

BADGE = {
    "sm": {"h": 22, "px": 10, "font": 11},
    "md": {"h": 26, "px": 12, "font": 12},
    "lg": {"h": 30, "px": 14, "font": 13},
}
DEFAULT_BADGE_SIZE = "sm"

# ==== TOKENS DE BORDA (Design System) ====
BORDER_WIDTH = 1
BORDER_RADIUS_PILL = 999

# ==============================
# INTEGRAÇÃO COM BANCO DE DADOS
# ==============================


def _compute_dashboard(atas: dict) -> dict:
    """Calcula métricas agregadas para o dashboard."""
    vigentes = len(atas.get("vigentes", []))
    vencidas = len(atas.get("vencidas", []))
    a_vencer = len(atas.get("aVencer", []))
    total = vigentes + vencidas + a_vencer
    total_cent = sum(
        db.parse_currency(a["valorTotal"])
        for lst in atas.values()
        for a in lst
    )
    return {
        "total": total,
        "valorTotal": db.format_currency(total_cent),
        "vigentes": vigentes,
        "aVencer": a_vencer,
    }


def _refresh_data(filters=None, search=None) -> None:
    """Atualiza os caches globais de atas e métricas."""
    global ATAS, DASHBOARD
    ATAS = db.fetch_atas(filters=filters, search=search)
    DASHBOARD = _compute_dashboard(ATAS)


db.init_db()
_refresh_data()


# ==============================
# UTILITÁRIOS: MÁSCARAS E VALIDAÇÕES
# ==============================
class MaskUtils:
    @staticmethod
    def _get_only_digits(text: str) -> str:
        return re.sub(r'\D', '', text)

    @staticmethod
    def aplicar_mascara_numero_ata(text: str) -> str:
        digits = MaskUtils._get_only_digits(text)
        if len(digits) > 8:
            digits = digits[:8]
        if len(digits) > 4:
            return f"{digits[:4]}/{digits[4:]}"
        return digits

    @staticmethod
    def aplicar_mascara_sei(text: str) -> str:
        digits = MaskUtils._get_only_digits(text)
        if len(digits) > 17:
            digits = digits[:17]
        
        if len(digits) > 15:
            return f"{digits[:5]}.{digits[5:11]}/{digits[11:15]}-{digits[15:]}"
        if len(digits) > 11:
            return f"{digits[:5]}.{digits[5:11]}/{digits[11:]}"
        if len(digits) > 5:
            return f"{digits[:5]}.{digits[5:]}"
        return digits

    @staticmethod
    def aplicar_mascara_telefone(text: str) -> str:
        digits = MaskUtils._get_only_digits(text)
        if len(digits) > 11:
            digits = digits[:11]

        if len(digits) > 10: # Celular (XX) XXXXX-XXXX
            return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
        if len(digits) > 6: # Telefone (XX) XXXX-XXXX
            return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
        if len(digits) > 2:
            return f"({digits[:2]}) {digits[2:]}"
        return digits

    @staticmethod
    def aplicar_mascara_data(text: str) -> str:
        digits = MaskUtils._get_only_digits(text)
        if len(digits) > 8:
            digits = digits[:8]
        if len(digits) > 4:
            return f"{digits[:2]}/{digits[2:4]}/{digits[4:]}"
        if len(digits) > 2:
            return f"{digits[:2]}/{digits[2:]}"
        return digits

class Validators:
    @staticmethod
    def validar_numero_ata(numero: str) -> bool:
        return bool(re.match(r'^\d{4}/\d{4}$', numero))

    @staticmethod
    def validar_documento_sei(documento: str) -> bool:
        return bool(re.match(r'^\d{5}\.\d{6}/\d{4}-\d{2}$', documento))

    @staticmethod
    def validar_telefone(telefone: str) -> bool:
        return bool(re.match(r'^\(\d{2}\)\s\d{4,5}-\d{4}$', telefone))

    @staticmethod
    def validar_email(email: str) -> bool:
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

    @staticmethod
    def validar_data_vigencia(data_str: str) -> Optional[date]:
        for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
            try:
                return datetime.strptime(data_str, fmt).date()
            except ValueError:
                pass
        return None

    @staticmethod
    def validar_valor_positivo(valor: str) -> Optional[float]:
        try:
            val = float(
                valor.replace("R$", "").replace(".", "").replace(",", ".").strip()
            )
            if val > 0:
                return val
        except (ValueError, TypeError):
            return None
        return None

    @staticmethod
    def validar_quantidade_positiva(quantidade: str) -> Optional[int]:
        try:
            qty = int(quantidade)
            if qty > 0:
                return qty
        except (ValueError, TypeError):
            return None
        return None


# ==============================
# APP
# ==============================
def main(page: ft.Page):
    page.title = "Painel - Dashboard + Atas (Flet)"
    page.padding = 0
    page.theme_mode = ft.ThemeMode.LIGHT
    page.bgcolor = TOKENS["colors"]["bg"]["app"]["light"]

    state = {
        "collapsed": True,
        "dark": False,
        "active": "dashboard",
        "filters": {key: False for key in FILTER_KEYS},
    }

    # --- REFs menu ---
    root = ft.Container()
    title_box = ft.Container()
    title_text = ft.Text()
    menu_icon = ft.Icon()
    divider_top = ft.Container()
    theme_icon = ft.Icon()
    theme_text_box = ft.Container()
    theme_text = ft.Text()
    items: dict[str, dict] = {}

    # Conteúdo principal
    content_col = ft.Column(expand=True, scroll=ft.ScrollMode.AUTO)
    content = ft.Container(expand=True, padding=20, content=content_col)

    # ---- Color getters (theme-aware) ----
    def _theme_key(): return "dark" if state["dark"] else "light"
    def is_dark(): return state["dark"]
    def is_collapsed(): return state["collapsed"]
    
    def border_token(): return TOKENS["colors"]["border"]["default"][_theme_key()]
    def surface_bg(): return TOKENS["colors"]["bg"]["surface"][_theme_key()]
    def text_color(): return TOKENS["colors"]["text"]["primary"][_theme_key()]
    def text_muted(): return TOKENS["colors"]["text"]["muted"][_theme_key()]
    def divider_color(): return TOKENS["colors"]["divider"]["default"][_theme_key()]

    # ---- Factory simples para TextField com borda padrão ----
    def tf(**kwargs):
        return ft.TextField(
            border_color=border_token(),
            border_width=BORDER_WIDTH,
            **kwargs
        )

    # ---------- Factory de botões PÍLULA (tema-aware) ----------
    def pill_button(
        text: str,
        icon: str | None = None,
        variant: str = "filled",
        size: str = DEFAULT_PILL_SIZE,
        on_click=None,
        expand: bool = False,
        disabled: bool = False,
        tooltip: str | None = None,
        style: ft.ButtonStyle | None = None,
    ):
        cfg = PILL.get(size, PILL["md"])
        if not style:
            style = ft.ButtonStyle(
                padding=ft.padding.symmetric(vertical=0, horizontal=cfg["px"]),
                shape=ft.RoundedRectangleBorder(radius=999),
                side=ft.BorderSide(BORDER_WIDTH, border_token()) if variant == "outlined" else None,
            )
        
        common = dict(
            text=text, icon=icon, style=style, height=cfg["h"],
            on_click=on_click, expand=expand, disabled=disabled, tooltip=tooltip
        )
        if variant == "outlined":
            return ft.OutlinedButton(**common)
        if variant == "text":
            return ft.TextButton(**common)
        if variant == "elevated":
            return ft.ElevatedButton(**common)
        return ft.FilledButton(**common)

    # ---------------- helpers gerais ----------------
    def set_content(view):
        content_col.controls = [view]
        page.update()

    # ---------------- MENU (com barrinha integrada) ----------------
    def update_item_visual(key: str):
        ref = items[key]
        active = state["active"] == key
        theme = _theme_key()

        ref["ink"].bgcolor = TOKENS["colors"]["component"]["sidebar"]["active"]["bg"][theme] if active else None
        ref["bar"].opacity = 1 if active else 0
        ref["bar"].bgcolor = TOKENS["colors"]["component"]["sidebar"]["active"]["bar"][theme]

        if is_collapsed():
            ref["text_box"].width = 0
            ref["text_box"].opacity = 0
            ref["text_box"].padding = 0
        else:
            ref["text_box"].width = W_EXPANDED - W_COLLAPSED - P_ITEM
            ref["text_box"].opacity = 1
            ref["text_box"].padding = ft.padding.only(right=8)

        if active:
            ref["icon"].color = TOKENS["colors"]["component"]["sidebar"]["active"]["text"][theme]
            ref["text"].color = TOKENS["colors"]["component"]["sidebar"]["active"]["text"][theme]
        else:
            ref["icon"].color = TOKENS["colors"]["component"]["sidebar"]["icon"]["inactive"][theme]
            ref["text"].color = text_muted()


    def set_active(key: str):
        state["active"] = key
        for k in items:
            update_item_visual(k)
        if key == "dashboard":
            set_content(DashboardView())
        elif key == "atas":
            set_content(AtasPage())
        elif key == "vencimentos":
            set_content(SimplePage("Vencimentos", "Veja suas atas que estão próximas de vencer."))
        elif key == "config":
            set_content(SimplePage("Configurações", "Gerencie as configurações do sistema."))
        page.update()

    def toggle_sidebar(_=None):
        state["collapsed"] = not state["collapsed"]
        root.width = W_COLLAPSED if is_collapsed() else W_EXPANDED
        menu_icon.rotate = ft.Rotate(0 if is_collapsed() else math.pi / 2, alignment=ft.alignment.center)

        if is_collapsed():
            title_box.width = 0
            title_text.opacity = 0
            theme_text_box.width = 0
            theme_text.opacity = 0
        else:
            title_box.width = W_EXPANDED - W_COLLAPSED - P_ITEM
            title_text.opacity = 1
            theme_text_box.width = W_EXPANDED - W_COLLAPSED - P_ITEM
            theme_text.opacity = 1

        for k in items:
            update_item_visual(k)
        page.update()

    def toggle_theme(_=None):
        state["dark"] = not state["dark"]
        theme = _theme_key()
        page.theme_mode = ft.ThemeMode.DARK if is_dark() else ft.ThemeMode.LIGHT
        page.bgcolor = TOKENS["colors"]["bg"]["app"][theme]
        theme_icon.name = "light_mode" if is_dark() else "dark_mode"
        theme_text.value = "Modo Claro" if is_dark() else "Modo Escuro"
        update_theme_Colors()

    def make_item(key: str, icon_name: str, label: str, active=False):
        icon = ft.Icon(icon_name, size=ICON_SIZE)
        txt = ft.Text(label, size=13, weight=ft.FontWeight.W_600, no_wrap=True)

        text_box = ft.Container(
            alignment=ft.alignment.center_left,
            content=txt,
            width=W_EXPANDED - W_COLLAPSED - P_ITEM,
            opacity=1,
            animate=ANIM,
            animate_opacity=300,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
        )

        row = ft.Row(
            controls=[icon, text_box],
            spacing=12,
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        )
        row_holder = ft.Container(content=row, padding=P_ITEM)

        bar = ft.Container(
            width=BAR_W,
            opacity=0,
            bgcolor=TOKENS["colors"]["component"]["sidebar"]["active"]["bar"]["light"],
            border_radius=ft.border_radius.only(top_left=R_ITEM, bottom_left=R_ITEM),
        )

        ink = ft.Container(
            content=ft.Stack(
                controls=[row_holder, ft.Container(alignment=ft.alignment.center_left, content=bar, expand=True)],
            ),
            border_radius=R_ITEM,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
            on_click=lambda _: set_active(key),
            tooltip=label,
        )

        wrapper = ft.Container(content=ink, border_radius=R_ITEM, animate=ANIM)

        items[key] = {"ink": ink, "bar": bar, "icon": icon, "text": txt, "text_box": text_box}
        if active:
            state["active"] = key
        update_item_visual(key)
        return wrapper

    # ---------------- THEME / Colors UPDATE ----------------
    def update_theme_Colors():
        theme = _theme_key()
        root.bgcolor = TOKENS["colors"]["component"]["sidebar"]["bg"][theme]
        divider_top.bgcolor = divider_color()
        title_text.color = text_color()
        
        menu_icon.color = TOKENS["colors"]["component"]["sidebar"]["icon"]["menu"][theme]
        theme_icon.color = TOKENS["colors"]["component"]["sidebar"]["icon"]["theme"][theme]

        for k in items:
            update_item_visual(k)
            
        # Atualiza a view atual para refletir as mudanças de cor
        if state["active"] == "dashboard":
            set_content(DashboardView())
        elif state["active"] == "atas":
            set_content(AtasPage())
        else:
            page.update()

    # ==============================
    # DASHBOARD VIEW
    # ==============================
    def StatCard(title: str, value: str, description: str, icon_name: str):
        return ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=TOKENS["colors"]["shadow"]["default"]),
            content=ft.Column(
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                alignment=ft.MainAxisAlignment.CENTER,
                controls=[
                    ft.Icon(icon_name, size=28, color=text_muted()),
                    ft.Text(title, size=12, color=text_muted(), weight=ft.FontWeight.W_500),
                    ft.Text(value, size=22, weight=ft.FontWeight.W_700, color=text_color()),
                    ft.Text(description, size=11, color=text_muted()),
                ],
                spacing=6,
            ),
            col={"xs": 12, "md": 6, "lg": 3},
        )

    def Donut():
        total = DASHBOARD["total"] or 1
        vig = DASHBOARD["vigentes"]
        av = DASHBOARD["aVencer"]
        ven = total - vig - av
        sections = [
            ft.PieChartSection(vig, title="", color=TOKENS["colors"]["chart"]["success"]),
            ft.PieChartSection(av, title="", color=TOKENS["colors"]["chart"]["warning"]),
            ft.PieChartSection(ven, title="", color=TOKENS["colors"]["chart"]["error"]),
        ]
        chart = ft.PieChart(
            sections=sections,
            center_space_radius=45,
            sections_space=2,
            animate=ft.Animation(300, "easeOut"),
        )
        legend = ft.Column(
            controls=[
                ft.Row([
                    ft.Container(width=8, height=8, bgcolor=TOKENS["colors"]["chart"]["success"], border_radius=20),
                    ft.Text(f"Vigentes: {vig} ({vig/total*100:.1f}%)", size=12, color=text_muted()),
                ], spacing=8),
                ft.Row([
                    ft.Container(width=8, height=8, bgcolor=TOKENS["colors"]["chart"]["warning"], border_radius=20),
                    ft.Text(f"A Vencer: {av} ({av/total*100:.1f}%)", size=12, color=text_muted()),
                ], spacing=8),
                ft.Row([
                    ft.Container(width=8, height=8, bgcolor=TOKENS["colors"]["chart"]["error"], border_radius=20),
                    ft.Text(f"Vencidas: {ven} ({ven/total*100:.1f}%)", size=12, color=text_muted()),
                ], spacing=8),
            ],
            spacing=6,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        )
        return ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(
                blur_radius=18, spread_radius=1, color=TOKENS["colors"]["shadow"]["default"]
            ),
            content=ft.Column(
                controls=[
                    ft.Text("Situação das Atas", size=16, weight=ft.FontWeight.W_600, color=text_color()),
                    ft.Container(content=chart, alignment=ft.alignment.center, padding=10),
                    ft.Container(content=legend, alignment=ft.alignment.center, padding=10),
                ],
                spacing=10,
            ),
            col={"xs": 12, "lg": 6},
        )

    def Bars():
        months = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
        values = [0, 0, 0, 0, 0, 0, 0, 0, 10, 45, 60, 0]
        groups = []
        chart_default_color = TOKENS["colors"]["chart"]["default"][_theme_key()]
        for i, v in enumerate(values):
            color = (TOKENS["colors"]["chart"]["warning"] if i == 9 else TOKENS["colors"]["chart"]["error"] if i == 10 else chart_default_color)
            groups.append(ft.BarChartGroup(x=i, bar_rods=[ft.BarChartRod(from_y=0, to_y=float(v), width=16, color=color, border_radius=4)]))
        chart = ft.BarChart(
            interactive=False, animate=ft.Animation(300, "easeOut"),
            max_y=70, min_y=0,
            bar_groups=groups,
            bottom_axis=ft.ChartAxis(labels=[ft.ChartAxisLabel(value=i, label=ft.Text(m, size=11, color=text_muted())) for i, m in enumerate(months)]),
            left_axis=ft.ChartAxis(show_labels=False),
            horizontal_grid_lines=ft.ChartGridLines(color=TOKENS["colors"]["shadow"]["faint"]),
        )
        return ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=TOKENS["colors"]["shadow"]["default"]),
            content=ft.Column(controls=[ft.Text("Vencimentos por Mês", size=16, weight=ft.FontWeight.W_600, color=text_color()), chart], spacing=10),
            col={"xs": 12, "lg": 6},
        )

    def WarningCard():
        dias_alerta = int(db.get_param("dias_alerta_vencimento", "60") or 60)
        theme = _theme_key()
        return ft.Container(
            bgcolor=TOKENS["colors"]["semantic"]["warning"]["bg"][theme],
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(
                blur_radius=18, spread_radius=1, color=TOKENS["colors"]["shadow"]["default"]
            ),
            border=ft.border.only(left=ft.BorderSide(4, TOKENS["colors"]["semantic"]["warning"]["border"])),
            content=ft.Column(
                controls=[
                    ft.Row(
                        [ft.Icon("warning", size=22, color=TOKENS["colors"]["semantic"]["warning"]["border"]), ft.Text("Atenção", weight=ft.FontWeight.W_600, color=text_color())],
                        spacing=8,
                    ),
                    ft.Text(
                        f"Você possui {DASHBOARD['aVencer']} ata(s) vencendo em {dias_alerta} dias ou menos.",
                        size=12,
                        color=text_muted(),
                    ),
                ],
                spacing=8,
            ),
            col=12,
        )

    def DashboardView():
        stats = [
            StatCard("Total de Atas", str(DASHBOARD["total"]), "cadastradas", "article"),
            StatCard("Valor Total", DASHBOARD["valorTotal"], "em atas", "payments"),
            StatCard("Vigentes", str(DASHBOARD["vigentes"]), f'{round(DASHBOARD["vigentes"]/DASHBOARD["total"]*100)}% do total', "check_circle"),
            StatCard("A Vencer", str(DASHBOARD["aVencer"]), f'{round(DASHBOARD["aVencer"]/DASHBOARD["total"]*100)}% do total', "schedule"),
        ]
        grid = ft.ResponsiveRow(
            controls=[*stats, Donut(), Bars(), WarningCard()],
            columns=12, run_spacing=16, spacing=16,
        )
        return grid

    # ==============================
    # ATAS: Tabela, Detalhes e Edição
    # ==============================
    def badge(text: str, variant: str, size: str = DEFAULT_BADGE_SIZE):
        size_cfg = BADGE.get(size, BADGE["sm"])
        theme = _theme_key()
        
        variant_key = "vencida" # fallback
        if variant == "green": variant_key = "vigente"
        elif variant == "amber": variant_key = "a_vencer"
        
        bg_final = TOKENS["colors"]["status"][variant_key]["bg"][theme]
        fg_final = TOKENS["colors"]["status"][variant_key]["text"][theme]

        return ft.Container(
            height=size_cfg["h"],
            padding=ft.padding.symmetric(vertical=0, horizontal=size_cfg["px"]),
            bgcolor=bg_final,
            border_radius=999,
            content=ft.Row(
                controls=[ft.Text(text, size=size_cfg["font"], weight=ft.FontWeight.W_600, color=fg_final)],
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=0,
            ),
        )

    def situacao_to_variant(s: str) -> str:
        s = (s or "").lower()
        if s == "vigente":
            return "green"
        if s == "a vencer":
            return "amber"
        return "red"

    def _perform_delete_ata(ata: dict):
        db.delete_ata_db(ata["id"])
        _refresh_data(state["filters"])
        show_snack("Ata excluída com sucesso!")
        set_content(AtasPage())
        
    def show_confirm_delete_modal(ata: dict):
        confirm_dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar Exclusão"),
            content=ft.Text(f"Tem certeza que deseja excluir a ata nº {ata.get('numero', '')}? Esta ação é irreversível."),
            actions_alignment=ft.MainAxisAlignment.END,
        )

        def handle_confirm(e):
            _perform_delete_ata(ata)
            page.close(confirm_dialog)

        def handle_cancel(e):
            page.close(confirm_dialog)

        confirm_dialog.actions = [
            pill_button("Cancelar", variant="text", on_click=handle_cancel),
            pill_button(
                "Excluir", 
                variant="filled",
                on_click=handle_confirm,
                style=ft.ButtonStyle(bgcolor=TOKENS["colors"]["semantic"]["error"]["bg_strong"], color=TOKENS["colors"]["text"]["inverse"])
            ),
        ]
        page.open(confirm_dialog)

    def AtasSectionCard(title: str, icon_name: str, data: list[dict], variant: str | None = None):
        # ===== resolve variant =====
        if not variant:
            t = (title or "").lower()
            if "vigentes" in t: variant = "green"
            elif "a vencer" in t or "à vencer" in t: variant = "amber"
            elif "vencidas" in t: variant = "red"
            else: variant = "red"
        variant_map = {"green": "vigente", "amber": "a_vencer", "red": "vencida"}
        variant_key = variant_map.get(variant, "vencida")
        theme = _theme_key()
        bg_color = TOKENS["colors"]["status"][variant_key]["bg"][theme]
        icon_color = TOKENS["colors"]["status"][variant_key]["text"][theme]

        # ===== helper: ícone clicável sem padding =====
        def action_icon(name: str, tooltip: str, on_click, color=None):
            return ft.Container(
                content=ft.Icon(name, size=18, color=color or text_color()),
                tooltip=tooltip,
                alignment=ft.alignment.center,
                padding=0,
                margin=0,
                border_radius=8,
                ink=True,
                on_click=on_click,
            )

        # ===== Header do card (ESQUERDA) =====
        header = ft.Row(
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=10,
            controls=[
                ft.Container(
                    width=28, height=28, border_radius=999, bgcolor=bg_color,
                    alignment=ft.alignment.center, padding=0, margin=0,
                    content=ft.Icon(icon_name, size=18, color=icon_color),
                ),
                ft.Text(title, size=16, weight=ft.FontWeight.W_600, color=text_color()),
            ],
        )

        # ===== Linhas (DataRow) =====
        rows_ui = []
        if not data:
            rows_ui.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(
                            ft.Container(
                                content=ft.Text("Nenhum registro.", color=text_muted()),
                                alignment=ft.alignment.center,
                                expand=True,
                                padding=0, margin=0,
                            ),
                            colspan=6,
                        )
                    ]
                )
            )
        else:
            for ata in data:
                rows_ui.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Container(ft.Text(ata.get("numero",""),     color=text_color()), alignment=ft.alignment.center, expand=True, padding=0, margin=0)),
                            ft.DataCell(ft.Container(ft.Text(ata.get("vigencia",""),   color=text_muted()), alignment=ft.alignment.center, expand=True, padding=0, margin=0)),
                            ft.DataCell(ft.Container(ft.Text(ata.get("objeto",""),     color=text_muted()), alignment=ft.alignment.center, expand=True, padding=0, margin=0)),
                            ft.DataCell(ft.Container(ft.Text(ata.get("fornecedor",""), color=text_muted()), alignment=ft.alignment.center, expand=True, padding=0, margin=0)),
                            ft.DataCell(
                                ft.Container(
                                    content=badge(ata["situacao"], situacao_to_variant(ata["situacao"]), size="md"),
                                    alignment=ft.alignment.center,
                                    expand=True,
                                    padding=0, margin=0,
                                )
                            ),
                            # === AÇÕES sem margem/padding (troca de IconButton -> Container clicável)
                            ft.DataCell(
                                ft.Container(
                                    alignment=ft.alignment.center,
                                    expand=True,
                                    padding=0, margin=0,
                                    content=ft.Row(
                                        tight=True,                          # ocupa só o necessário
                                        spacing=6,                           # apenas espaço entre ícones
                                        alignment=ft.MainAxisAlignment.CENTER,
                                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                                        controls=[
                                            action_icon("visibility", "Ver",    lambda e, a=ata: show_ata_details(a)),
                                            action_icon("edit",       "Editar", lambda e, a=ata: show_ata_edit(a)),
                                            action_icon("delete",     "Excluir", lambda e, a=ata: show_confirm_delete_modal(a),
                                                        color=TOKENS["colors"]["semantic"]["error"]["icon"]),
                                        ],
                                    ),
                                )
                            ),
                        ]
                    )
                )

        # ===== DataTable (sem margens, cabeçalhos centralizados) =====
        table = ft.DataTable(
            expand=True,
            column_spacing=16,                 # espaço ENTRE colunas; não é margem lateral
            horizontal_margin=0,               # zera margem da tabela
            checkbox_horizontal_margin=0,      # zera margem da col. de checkbox (mesmo não usando)
            heading_row_color=TOKENS["colors"]["bg"]["surface"]["muted"],
            vertical_lines=ft.BorderSide(1.5, border_token()),
            horizontal_lines=ft.BorderSide(BORDER_WIDTH, border_token()),
            border=ft.border.all(BORDER_WIDTH, border_token()),
            border_radius=8,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
            columns=[
                ft.DataColumn(ft.Text("NÚMERO",    size=11, color=text_muted(), weight=ft.FontWeight.W_600), heading_row_alignment=ft.MainAxisAlignment.CENTER, numeric=False),
                ft.DataColumn(ft.Text("VIGÊNCIA",   size=11, color=text_muted(), weight=ft.FontWeight.W_600), heading_row_alignment=ft.MainAxisAlignment.CENTER, numeric=False),
                ft.DataColumn(ft.Text("OBJETO",     size=11, color=text_muted(), weight=ft.FontWeight.W_600), heading_row_alignment=ft.MainAxisAlignment.CENTER, numeric=False),
                ft.DataColumn(ft.Text("FORNECEDOR", size=11, color=text_muted(), weight=ft.FontWeight.W_600), heading_row_alignment=ft.MainAxisAlignment.CENTER, numeric=False),
                ft.DataColumn(ft.Text("SITUAÇÃO",   size=11, color=text_muted(), weight=ft.FontWeight.W_600), heading_row_alignment=ft.MainAxisAlignment.CENTER, numeric=False),
                ft.DataColumn(ft.Text("AÇÕES",      size=11, color=text_muted(), weight=ft.FontWeight.W_600), heading_row_alignment=ft.MainAxisAlignment.CENTER, numeric=False),
            ],
            rows=rows_ui,
        )

        # ===== Container do card =====
        return ft.Container(
            col=12,
            bgcolor=surface_bg(),
            border_radius=16,
            padding=16,
            shadow=ft.BoxShadow(blur_radius=16, spread_radius=1, color=TOKENS["colors"]["shadow"]["soft"]),
            content=ft.Column(
                spacing=10,
                controls=[
                    header,
                    ft.Row(controls=[table], expand=True),
                ],
            ),
        )



    def _filters_count() -> int:
        return sum(state["filters"].values())

    def _filter_label() -> str:
        n = _filters_count()
        return f"Filtrar ({n})" if n else "Filtrar"
    
    def AtasPage():
        # ----- helper: botão redondo com ícone (hover/ink 100% circulares) -----
        def round_icon_button(icon_name: str, tooltip: str, on_click=None):
            return ft.Container(
                width=40,
                height=40,
                alignment=ft.alignment.center,
                border_radius=999,
                bgcolor=TOKENS["colors"]["bg"]["input"]["default"],
                border=ft.border.all(BORDER_WIDTH, border_token()),
                content=ft.Icon(icon_name, size=20, color=text_color()),
                ink=True,
                on_click=on_click,
                tooltip=tooltip,
                clip_behavior=ft.ClipBehavior.HARD_EDGE,
            )

        # ----- Busca -----
        input_padding = ft.padding.symmetric(vertical=0, horizontal=PILL["md"]["px"])
        search = tf(
            hint_text="Buscar atas...",
            prefix_icon=ft.Icons.SEARCH,
            border_radius=BORDER_RADIUS_PILL,
            content_padding=input_padding,
            bgcolor=TOKENS["colors"]["bg"]["input"]["default"],
            height=PILL["md"]["h"],
            expand=True,
        )

        # Contêiner raiz desta view (não chame update antes de estar anexado)
        root_row = ft.ResponsiveRow(columns=12, spacing=16, run_spacing=16)

        def build_cards():
            filter_state = state.get("filters", {key: False for key in FILTER_KEYS})
            show_all = not any(filter_state.values())
            cards = []

            filter_map = {
                'vigente':   {'title': 'Atas Vigentes', 'icon': 'check_circle', 'data': ATAS['vigentes'], 'variant': 'green'},
                'vencida':   {'title': 'Atas Vencidas', 'icon': 'cancel',       'data': ATAS['vencidas'], 'variant': 'red'},
                'a_vencer':  {'title': 'Atas a Vencer', 'icon': 'schedule',     'data': ATAS['aVencer'],  'variant': 'amber'},
            }

            for key in FILTER_KEYS:
                if show_all or filter_state.get(key):
                    info = filter_map[key]
                    cards.append(AtasSectionCard(info['title'], info['icon'], info['data'], variant=info['variant']))
            return cards

        # ====== SUBMENU que NÃO fecha ao marcar filtros ======
        filter_btn_ref: ft.Ref[ft.Container] = ft.Ref[ft.Container]()

        def _filters_count() -> int:
            return sum(state["filters"].values())

        def _filter_label() -> str:
            n = _filters_count()
            return f"Filtrar ({n})" if n else "Filtrar"

        def _update_filter_tooltip():
            btn = filter_btn_ref.current
            # Só atualiza se o botão já está anexado ao page
            if btn and getattr(btn, "page", None):
                btn.tooltip = _filter_label()
                btn.update()

        def _check_icon(flag: bool) -> ft.Icon:
            return ft.Icon(ft.Icons.CHECK_BOX if flag else ft.Icons.CHECK_BOX_OUTLINE_BLANK, size=18)

        # Handlers de clique nos itens do menu
        def _toggle_flag(key: str, item: ft.MenuItemButton):
            state["filters"][key] = not state["filters"][key]
            item.leading = _check_icon(state["filters"][key])
            item.update()
            _update_filter_tooltip()

        def _on_filter_clear(e):
            # Limpar NÃO fecha o menu nem aplica; só zera os checks
            for k in FILTER_KEYS:
                state["filters"][k] = False
            mi_vigente.leading = _check_icon(False); mi_vigente.update()
            mi_vencida.leading = _check_icon(False); mi_vencida.update()
            mi_a_vencer.leading = _check_icon(False); mi_a_vencer.update()
            _update_filter_tooltip()

        def _on_filter_apply(e):
            # Aplicar fecha (close_on_click=True) e ATUALIZA os cards
            _refresh_data(state["filters"], search.value or None)
            _update_filter_tooltip()
            rebuild()

        # Itens do menu com ícone inicial já definido (sem update aqui!)
        mi_vigente = ft.MenuItemButton(
            close_on_click=False,
            leading=_check_icon(state["filters"]["vigente"]),
            content=ft.Text("Vigentes"),
        )
        mi_vencida = ft.MenuItemButton(
            close_on_click=False,
            leading=_check_icon(state["filters"]["vencida"]),
            content=ft.Text("Vencidas"),
        )
        mi_a_vencer = ft.MenuItemButton(
            close_on_click=False,
            leading=_check_icon(state["filters"]["a_vencer"]),
            content=ft.Text("A Vencer"),
        )
        # Amarra on_click depois de criados (serão clicados já no page)
        mi_vigente.on_click = lambda e: _toggle_flag("vigente", mi_vigente)
        mi_vencida.on_click = lambda e: _toggle_flag("vencida", mi_vencida)
        mi_a_vencer.on_click = lambda e: _toggle_flag("a_vencer", mi_a_vencer)

        mi_apply = ft.MenuItemButton(
            close_on_click=True,  # fecha ao aplicar
            leading=ft.Icon(ft.Icons.DONE, size=18),
            content=ft.Text("Aplicar"),
            on_click=_on_filter_apply,
        )
        mi_clear = ft.MenuItemButton(
            close_on_click=False,  # NÃO fecha
            leading=ft.Icon(ft.Icons.CLEAR_ALL, size=18),
            content=ft.Text("Limpar"),
            on_click=_on_filter_clear,
        )
        mi_close = ft.MenuItemButton(
            close_on_click=True,  # fecha sem aplicar
            leading=ft.Icon(ft.Icons.CLOSE, size=18),
            content=ft.Text("Fechar"),
        )

        # Botão circular 40×40 com SubmenuButton centralizado
        filter_btn = ft.Container(
            ref=filter_btn_ref,
            width=40,
            height=40,
            alignment=ft.alignment.center,
            border_radius=999,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
            border=ft.border.all(BORDER_WIDTH, border_token()),
            bgcolor=TOKENS["colors"]["bg"]["input"]["default"],
            tooltip=_filter_label(),
            content=ft.SubmenuButton(
                style=ft.ButtonStyle(
                    padding=ft.padding.all(0),
                    shape=ft.RoundedRectangleBorder(radius=999),
                ),
                content=ft.Icon(ft.Icons.FILTER_LIST, size=20, color=text_color()),
                controls=[mi_vigente, mi_vencida, mi_a_vencer, mi_apply, mi_clear, mi_close],
            ),
        )

        # --- outros botões circulares (mantidos) ---
        sort_btn = round_icon_button("sort", "Ordenar")
        new_btn = round_icon_button("add", "Nova Ata", on_click=lambda _: show_ata_edit({}))

        actions = ft.Row(
            controls=[filter_btn, sort_btn, new_btn],
            spacing=8,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        )

        # Topo (criado uma vez; reaproveitado em rebuild)
        top_container = ft.Container(
            col=12,
            bgcolor=surface_bg(),
            border_radius=16,
            padding=16,
            shadow=ft.BoxShadow(blur_radius=12, spread_radius=1, color=TOKENS["colors"]["shadow"]["faint"]),
            content=ft.Row(
                controls=[ft.Container(content=search, expand=True), actions],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=12,
            ),
        )

        def rebuild():
            # Reconstrói conteúdo; só chama update se já estiver no page
            root_row.controls = [top_container, *build_cards()]
            if getattr(root_row, "page", None):
                root_row.update()

        def _on_search(e):
            _refresh_data(state["filters"], search.value or None)
            rebuild()

        search.on_submit = _on_search

        # Carrega dados iniciais e compõe SEM update (a view ainda não foi anexada)
        _refresh_data(state["filters"], search.value or None)
        root_row.controls = [top_container, *build_cards()]
        return root_row



        # ====== PopupMenuButton (substitui MenuAnchor do botão de filtro) ======
        pb_ref: ft.Ref[ft.PopupMenuButton] = ft.Ref[ft.PopupMenuButton]()

        def _update_pb_tooltip():
            if pb_ref.current:
                pb_ref.current.tooltip = _filter_label()
                pb_ref.current.update()

        def _apply_filters_and_refresh():
            _refresh_data(state["filters"], search.value or None)
            _update_pb_tooltip()
            set_content(AtasPage())

        # Handlers de toggle para cada item de filtro
        def toggle_vigente(e):
            state["filters"]["vigente"] = not state["filters"]["vigente"]
            e.control.checked = state["filters"]["vigente"]
            page.update()
            _update_pb_tooltip()

        def toggle_vencida(e):
            state["filters"]["vencida"] = not state["filters"]["vencida"]
            e.control.checked = state["filters"]["vencida"]
            page.update()
            _update_pb_tooltip()

        def toggle_a_vencer(e):
            state["filters"]["a_vencer"] = not state["filters"]["a_vencer"]
            e.control.checked = state["filters"]["a_vencer"]
            page.update()
            _update_pb_tooltip()

        # --- ITENS DO POPUP CENTRALIZADOS ---
        pm_vigente = ft.PopupMenuItem(
            checked=state["filters"]["vigente"],
            on_click=toggle_vigente,
            content=ft.Row(
                controls=[ft.Icon(ft.Icons.CHECK_CIRCLE, size=18), ft.Text("Vigentes")],
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
        )
        pm_vencida = ft.PopupMenuItem(
            checked=state["filters"]["vencida"],
            on_click=toggle_vencida,
            content=ft.Row(
                controls=[ft.Icon(ft.Icons.CANCEL, size=18), ft.Text("Vencidas")],
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
        )
        pm_a_vencer = ft.PopupMenuItem(
            checked=state["filters"]["a_vencer"],
            on_click=toggle_a_vencer,
            content=ft.Row(
                controls=[ft.Icon(ft.Icons.SCHEDULE, size=18), ft.Text("A Vencer")],
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
        )
        def _on_filter_clear(e):
            state["filters"] = {key: False for key in FILTER_KEYS}
            pm_vigente.checked = False
            pm_vencida.checked = False
            pm_a_vencer.checked = False
            page.update()
            _update_pb_tooltip()

        def _on_filter_apply(e):
            _apply_filters_and_refresh()

        pm_apply = ft.PopupMenuItem(
            on_click=_on_filter_apply,
            content=ft.Row(
                controls=[ft.Icon(ft.Icons.DONE, size=18), ft.Text("Aplicar")],
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
        )
        pm_clear = ft.PopupMenuItem(
            on_click=_on_filter_clear,
            content=ft.Row(
                controls=[ft.Icon(ft.Icons.CLEAR_ALL, size=18), ft.Text("Limpar")],
                alignment=ft.MainAxisAlignment.CENTER,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
        )

# --- BOTÃO DE FILTRO COM HOVER CIRCULAR (usa icon=, não content=) ---
        filter_btn = ft.Container(
            width=40,
            height=40,
            alignment=ft.alignment.center,
            border_radius=999,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,  # mantém o visual do círculo
            border=ft.border.all(BORDER_WIDTH, border_token()),
            bgcolor=TOKENS["colors"]["bg"]["input"]["default"],
            content=ft.PopupMenuButton(
                ref=pb_ref,
                tooltip=_filter_label(),
                items=[
                    pm_vigente,
                    pm_vencida,
                    pm_a_vencer,
                    ft.PopupMenuItem(),  # divisor
                    pm_apply,
                    pm_clear,
                ],
                # <- ESTE é o ponto: usar 'icon' em vez de 'content'
                icon=ft.Icons.FILTER_LIST,
                icon_color=text_color(),
                icon_size=20,
                padding=ft.padding.all(0),  # 40x40 exato, sem “gordura”
                splash_radius=20,           # splash/hover circular
            ),
        )




        # --- outros botões circulares (mantidos) ---
        sort_btn = round_icon_button("sort", "Ordenar")
        new_btn = round_icon_button("add", "Nova Ata", on_click=lambda _: show_ata_edit({}))

        actions = ft.Row(
            controls=[filter_btn, sort_btn, new_btn],  # <- usa filter_btn aqui
            spacing=8,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        )


        top = ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=16,
            shadow=ft.BoxShadow(blur_radius=12, spread_radius=1, color=TOKENS["colors"]["shadow"]["faint"]),
            content=ft.Row(
                controls=[ft.Container(content=search, expand=True), actions],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=12,
            ),
        )

        filter_state = state.get("filters", {key: False for key in FILTER_KEYS})
        show_all = not any(filter_state.values())
        cards = []

        filter_map = {
            'vigente':   {'title': 'Atas Vigentes', 'icon': 'check_circle', 'data': ATAS['vigentes'], 'variant': 'green'},
            'vencida':   {'title': 'Atas Vencidas', 'icon': 'cancel',       'data': ATAS['vencidas'], 'variant': 'red'},
            'a_vencer':  {'title': 'Atas a Vencer', 'icon': 'schedule',     'data': ATAS['aVencer'],  'variant': 'amber'},
        }

        for key in FILTER_KEYS:
            if show_all or filter_state.get(key):
                info = filter_map[key]
                cards.append(AtasSectionCard(info['title'], info['icon'], info['data'], variant=info['variant']))

        grid = ft.ResponsiveRow(
            columns=12,
            spacing=16,
            run_spacing=16,
            controls=[ft.Container(content=top, col=12), *cards],
        )
        return grid



    def show_ata_details(ata: dict):
        title = ft.Text("Ata de Registro de Preços", size=20, weight=ft.FontWeight.W_700, color=text_color())
        subtitle = ft.Text(f"Nº {ata['numero']}", size=13, color=text_muted())

        header = ft.Row(
            controls=[
                ft.Column(controls=[title, subtitle], spacing=2, expand=True),
                ft.Row(
                    controls=[
                        pill_button("Voltar", icon="arrow_back", variant="outlined", on_click=lambda e: set_content(AtasPage())),
                        pill_button("Editar", icon="edit", variant="filled", on_click=lambda e, ata_=ata: show_ata_edit(ata_)),
                    ],
                    spacing=8,
                ),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )

        dados = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=ft.Column(
                controls=[
                    ft.Row([ft.Icon("article", color=text_muted()), ft.Text("Dados Gerais", size=16, weight=ft.FontWeight.W_600, color=text_color())], spacing=8),
                    ft.Column(controls=[
                        ft.Text(f"Documento SEI: {ata.get('documentoSei') or '—'}", color=text_muted()),
                        ft.Text(f"Objeto: {ata.get('objeto')}", color=text_muted()),
                        ft.Text(f"Vigência: {ata.get('vigencia')}", color=text_muted()),
                    ], spacing=6)
                ], spacing=10
            ),
        )
        forn = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=ft.Column(
                controls=[
                    ft.Row([ft.Icon("person", color=text_muted()), ft.Text("Fornecedor", size=16, weight=ft.FontWeight.W_600, color=text_color())], spacing=8),
                    ft.Column(controls=[
                        ft.Text(f"Nome: {ata.get('fornecedor')}", color=text_muted()),
                        ft.Row([ft.Icon("call", size=16, color=text_muted()), ft.Text(", ".join(ata.get('contatos', {}).get('telefone') or ['—']), color=text_muted())]),
                        ft.Row([ft.Icon("mail", size=16, color=text_muted()), ft.Text(", ".join(ata.get('contatos', {}).get('email') or ['—']), color=text_muted())]),
                    ], spacing=6)
                ], spacing=10
            ),
        )
        grid_top = ft.ResponsiveRow(columns=12, spacing=16, run_spacing=16, controls=[
            ft.Container(content=dados, col={"xs": 12, "lg": 6}),
            ft.Container(content=forn, col={"xs": 12, "lg": 6}),
        ])

        # (Tabela de itens mantém DataTable anterior)
        it_cols = [
            ft.DataColumn(ft.Text("Descrição", color=text_muted())),
            ft.DataColumn(ft.Text("Qtd.", color=text_muted())),
            ft.DataColumn(ft.Text("Valor Unit.", color=text_muted())),
            ft.DataColumn(ft.Text("Subtotal", color=text_muted())),
        ]
        it_rows = [ft.DataRow(cells=[
            ft.DataCell(ft.Text(i["descricao"], color=text_color())),
            ft.DataCell(ft.Text(str(i["quantidade"]), color=text_muted())),
            ft.DataCell(ft.Text(i["valorUnitario"], color=text_muted())),
            ft.DataCell(ft.Text(i["subtotal"], color=text_muted())),
        ]) for i in (ata.get("itens") or [])]
        itens_table = ft.DataTable(columns=it_cols, rows=it_rows, column_spacing=24, divider_thickness=0.7)

        itens_card = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=ft.Column(controls=[
                ft.Row([ft.Icon("list_alt", color=text_muted()), ft.Text("Itens da Ata", size=16, weight=ft.FontWeight.W_600, color=text_color())], spacing=8),
                ft.Container(content=itens_table),
                ft.Container(
                    padding=ft.padding.only(top=12),
                    content=ft.Row(controls=[ft.Text("Valor Total", weight=ft.FontWeight.W_600, color=text_muted()),
                                            ft.Text(ata["valorTotal"], weight=ft.FontWeight.W_600, color=text_muted())],
                                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
                ),
            ], spacing=10),
        )

        view = ft.Column(controls=[ft.Container(content=header, padding=0), grid_top, itens_card], spacing=16)
        set_content(view)

    def show_ata_edit(ata: dict):
        is_new = not bool(ata)
        
        numero = tf(label="Número da Ata", value=ata.get("numero", ""), hint_text="0000/0000")
        documento_sei = tf(label="Documento SEI", value=ata.get("documentoSei", ""), hint_text="00000.000000/0000-00")
        data_vigencia = tf(label="Data de Vigência", value=ata.get("vigencia", ""), hint_text="DD/MM/AAAA")
        objeto = tf(label="Objeto", value=ata.get("objeto", ""))
        fornecedor = tf(label="Fornecedor", value=ata.get("fornecedor", ""))

        def on_num_change(e):
            e.control.value = MaskUtils.aplicar_mascara_numero_ata(e.control.value)
            e.control.update()
        numero.on_change = on_num_change

        def on_sei_change(e):
            e.control.value = MaskUtils.aplicar_mascara_sei(e.control.value)
            e.control.update()
        documento_sei.on_change = on_sei_change

        def on_date_change(e):
            e.control.value = MaskUtils.aplicar_mascara_data(e.control.value)
            e.control.update()
        data_vigencia.on_change = on_date_change

        def on_tel_change(e):
            e.control.value = MaskUtils.aplicar_mascara_telefone(e.control.value)
            e.control.update()

        tels_data = ata.get("contatos", {}).get("telefone", [""])
        tels_controls = [tf(label=f"Telefone {i+1}", value=v, prefix_icon=ft.Icons.PHONE, on_change=on_tel_change, hint_text="(XX) XXXXX-XXXX") for i, v in enumerate(tels_data)]
        
        emails_data = ata.get("contatos", {}).get("email", [""])
        emails_controls = [tf(label=f"E-mail {i+1}", value=v, prefix_icon=ft.Icons.MAIL, hint_text="exemplo@email.com") for i, v in enumerate(emails_data)]

        itens_data = ata.get("itens", [])[:] if ata.get("itens") else [{"descricao": "", "quantidade": "", "valorUnitario": ""}]
        itens_fields_controls = []

        def validate_form(e):
            all_fields = [numero, documento_sei, data_vigencia, objeto, fornecedor] + tels_controls + emails_controls + [item for row in itens_fields_controls for item in row.controls if isinstance(item, ft.TextField)]
            for field in all_fields: field.error_text = None

            is_valid = True
            if not numero.value or not Validators.validar_numero_ata(numero.value):
                numero.error_text = "Formato esperado: 0000/0000"; is_valid = False
            if not documento_sei.value or not Validators.validar_documento_sei(documento_sei.value):
                documento_sei.error_text = "Formato esperado: 00000.000000/0000-00"; is_valid = False
            if not data_vigencia.value or not Validators.validar_data_vigencia(data_vigencia.value):
                data_vigencia.error_text = "Data inválida. Use DD/MM/AAAA."; is_valid = False
            if not objeto.value.strip():
                objeto.error_text = "O objeto não pode ser vazio."; is_valid = False
            if not fornecedor.value.strip():
                fornecedor.error_text = "O fornecedor não pode ser vazio."; is_valid = False

            if not [tel for tel in tels_controls if tel.value and Validators.validar_telefone(tel.value)]:
                is_valid = False
                for tel in tels_controls:
                    if not tel.value or not Validators.validar_telefone(tel.value): tel.error_text = "Telefone inválido ou vazio."

            if not [email for email in emails_controls if email.value and Validators.validar_email(email.value)]:
                is_valid = False
                for email in emails_controls:
                    if not email.value or not Validators.validar_email(email.value): email.error_text = "E-mail inválido ou vazio."
            
            for row in itens_fields_controls:
                desc_field, qtd_field, vu_field = row.controls[0], row.controls[1], row.controls[2]
                if not desc_field.value.strip(): desc_field.error_text = "Obrigatório"; is_valid = False
                if not Validators.validar_quantidade_positiva(qtd_field.value): qtd_field.error_text = "Inválido"; is_valid = False
                if not Validators.validar_valor_positivo(vu_field.value): vu_field.error_text = "Inválido"; is_valid = False

            page.update()
            if is_valid:
                vigencia_dt = Validators.validar_data_vigencia(data_vigencia.value)
                forn_id = db.get_or_create_fornecedor(fornecedor.value.strip())

                itens = []
                for row in itens_fields_controls:
                    desc_field, qtd_field, vu_field = row.controls[0], row.controls[1], row.controls[2]
                    itens.append(
                        {
                            "descricao": desc_field.value.strip(),
                            "quantidade": int(qtd_field.value),
                            "valor_unit_centavos": db.parse_currency(vu_field.value),
                        }
                    )

                contatos = []
                for tel in tels_controls:
                    if tel.value:
                        contatos.append({"tipo": "telefone", "valor": tel.value})
                for em in emails_controls:
                    if em.value:
                        contatos.append({"tipo": "email", "valor": em.value})

                dto = {
                    "numero": numero.value.strip(),
                    "sei": documento_sei.value.strip(),
                    "objeto": objeto.value.strip(),
                    "fornecedor_id": forn_id,
                    "data_inicio": vigencia_dt.isoformat(),
                    "data_fim": vigencia_dt.isoformat(),
                    "itens": itens,
                    "contatos": contatos,
                }

                try:
                    if is_new:
                        db.create_ata(dto)
                    else:
                        db.update_ata(ata["id"], dto)
                except sqlite3.IntegrityError:
                    show_snack("Já existe uma ata com este número SEI.", error=True)
                    return

                _refresh_data()
                show_snack("Ata salva com sucesso!")
                set_content(AtasPage())

        def build_item_row(idx, item_data):
            desc = tf(label="Descrição", value=item_data.get("descricao", ""), expand=True)
            qtd = tf(label="Qtd.", value=str(item_data.get("quantidade", "")), width=80)
            vu = tf(label="Valor Unit.", value=item_data.get("valorUnitario", ""), width=120)
            
            def delete_item_row(e, row_to_delete):
                itens_fields_controls.remove(row_to_delete); refresh_ui()

            row = ft.Row([desc, qtd, vu], spacing=8, alignment=ft.MainAxisAlignment.START)
            del_btn = ft.IconButton(icon="delete", tooltip="Excluir", on_click=lambda e, r=row: delete_item_row(e, r))
            row.controls.append(del_btn)
            return row

        for i, it in enumerate(itens_data):
            itens_fields_controls.append(build_item_row(i, it))

        def add_tel(e):
            tels_controls.append(tf(label=f"Telefone {len(tels_controls)+1}", value="", prefix_icon=ft.Icons.PHONE, on_change=on_tel_change, hint_text="(XX) XXXXX-XXXX")); refresh_ui()

        def add_email(e):
            emails_controls.append(tf(label=f"E-mail {len(emails_controls)+1}", value="", prefix_icon=ft.Icons.MAIL, hint_text="exemplo@email.com")); refresh_ui()
        
        def add_item(e):
            itens_fields_controls.append(build_item_row(len(itens_fields_controls), {})); refresh_ui()

        def refresh_ui():
            def create_deletable_row(ctrl_list, ctrl, index):
                def delete_control(e): ctrl_list.pop(index); refresh_ui()
                return ft.Row([ctrl, ft.IconButton(icon="delete", tooltip="Excluir", on_click=delete_control)], spacing=8, alignment=ft.MainAxisAlignment.START)
            
            for i, tel in enumerate(tels_controls): tel.label = f"Telefone {i+1}"
            for i, email in enumerate(emails_controls): email.label = f"E-mail {i+1}"

            contacts_col.controls = [
                ft.Row([ft.Text("Contatos", size=16, weight=ft.FontWeight.W_600, color=text_color()), ft.Row([pill_button("Adicionar telefone", icon="add", variant="text", size="sm", on_click=add_tel), pill_button("Adicionar e-mail", icon="add", variant="text", size="sm", on_click=add_email)], spacing=4)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Column([create_deletable_row(tels_controls, t, i) for i, t in enumerate(tels_controls)], spacing=8),
                ft.Column([create_deletable_row(emails_controls, m, i) for i, m in enumerate(emails_controls)], spacing=8),
            ]
            
            itens_col.controls = [
                ft.Row([ft.Text("Itens", size=16, weight=ft.FontWeight.W_600, color=text_color()), pill_button("Adicionar", icon="add", variant="outlined", size="sm", on_click=add_item)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Column(itens_fields_controls, spacing=8)
            ]
            page.update()

        header = ft.Row(
            controls=[
                ft.Column([ft.Text("Ata de Registro de Preços", size=20, weight=ft.FontWeight.W_700, color=text_color()), ft.Text("Editar Ata" if not is_new else "Nova Ata", size=13, color=text_muted())], spacing=2, expand=True),
                ft.Row([pill_button("Voltar", icon="arrow_back", variant="outlined", on_click=lambda e: set_content(AtasPage())), pill_button("Salvar", icon="save", variant="filled", on_click=validate_form)], spacing=8),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN
        )

        dados_gerais = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=ft.Column(controls=[ft.Text("Dados Gerais", size=16, weight=ft.FontWeight.W_600, color=text_color()), numero, documento_sei, data_vigencia, objeto, fornecedor], spacing=10),
        )

        contacts_col = ft.Column(spacing=10)
        contatos_card = ft.Container(bgcolor=surface_bg(), border_radius=16, padding=16, content=contacts_col)

        grid_top = ft.ResponsiveRow(
            columns=12, spacing=16, run_spacing=16,
            controls=[ft.Container(content=dados_gerais, col={"xs": 12, "lg": 6}), ft.Container(content=contatos_card, col={"xs": 12, "lg": 6})]
        )

        itens_col = ft.Column(spacing=10)
        itens_card = ft.Container(bgcolor=surface_bg(), border_radius=16, padding=16, content=itens_col)

        view = ft.Column(controls=[header, grid_top, itens_card], spacing=16)
        set_content(view)
        refresh_ui()

    def show_snack(msg: str, error: bool = False):
        color = TOKENS["colors"]["semantic"]["error"]["bg"] if error else TOKENS["colors"]["semantic"]["success"]["bg"]
        page.snack_bar = ft.SnackBar(ft.Text(msg), bgcolor=color)
        page.snack_bar.open = True
        page.update()

    def SimplePage(title: str, subtitle: str):
        return ft.Column(controls=[
            ft.Container(
                bgcolor=surface_bg(), border_radius=16, padding=16,
                content=ft.Column(controls=[ft.Text(title, size=18, weight=ft.FontWeight.W_600, color=text_color()), ft.Text(subtitle, color=text_muted())], spacing=6))
        ])

    # ==============================
    # ASIDE (MENU)
    # ==============================
    top_logo = ft.Container(height=56, alignment=ft.alignment.center, content=ft.Icon("diamond", size=ICON_SIZE, color=TOKENS["colors"]["component"]["sidebar"]["icon"]["logo"]), padding=ft.padding.only(top=8, bottom=8))
    
    menu_icon = ft.Icon(
        "menu",
        size=ICON_SIZE,
        color=TOKENS["colors"]["component"]["sidebar"]["icon"]["menu"]["light"],
        rotate=ft.Rotate(0, alignment=ft.alignment.center),
        animate_rotation=ANIM
    )
    header_btn = ft.Container(
        content=ft.Row(
            controls=[
                menu_icon,
                (title_box := ft.Container(
                    alignment=ft.alignment.center_left,
                    content=(title_text := ft.Text("Painel", size=18, weight=ft.FontWeight.BOLD, no_wrap=True, opacity=0)),
                    width=0, animate=ANIM, animate_opacity=300, clip_behavior=ft.ClipBehavior.HARD_EDGE,
                )),
            ],
            spacing=12, alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        padding=P_ITEM, border_radius=R_ITEM, on_click=toggle_sidebar, tooltip="Expandir/recolher",
    )
    divider_top = ft.Container(height=1)

    nav = ft.Column(
        controls=[
            make_item("dashboard", "home", "Início", active=True),
            make_item("atas", "article", "Atas"),
            make_item("vencimentos", "timer", "Vencimentos"),
            make_item("config", "settings", "Configurações"),
        ],
        spacing=8, expand=True, scroll=ft.ScrollMode.AUTO,
    )

    theme_icon = ft.Icon("dark_mode", size=ICON_SIZE, color=TOKENS["colors"]["component"]["sidebar"]["icon"]["theme"]["light"])
    theme_text = ft.Text("Modo Escuro", size=13, weight=ft.FontWeight.W_600, no_wrap=True, opacity=0)
    theme_text_box = ft.Container(
        alignment=ft.alignment.center_left, content=theme_text,
        width=0, animate=ANIM, animate_opacity=300, clip_behavior=ft.ClipBehavior.HARD_EDGE,
    )
    theme_btn = ft.Container(
        content=ft.Row(controls=[theme_icon, theme_text_box], spacing=12, alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER),
        padding=P_ITEM, border_radius=R_ITEM, on_click=toggle_theme, tooltip="Alternar tema",
    )

    root = ft.Container(
        width=W_COLLAPSED, bgcolor=TOKENS["colors"]["component"]["sidebar"]["bg"]["light"],
        border_radius=ft.border_radius.only(top_right=RADIUS_ASIDE, bottom_right=RADIUS_ASIDE),
        padding=P_ROOT,
        content=ft.Column(
            controls=[top_logo, header_btn, divider_top, ft.Container(height=8), nav, ft.Container(height=8), ft.Container(height=1), theme_btn],
            expand=True, spacing=0,
        ),
        animate=ANIM,
        shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=TOKENS["colors"]["shadow"]["strong"]),
    )

    def init_state():
        root.width = W_COLLAPSED
        title_box.width = 0
        title_text.opacity = 0
        theme_text_box.width = 0
        theme_text.opacity = 0
        for k in items:
            update_item_visual(k)
    init_state()
    set_content(DashboardView())
    update_theme_Colors()

    page.add(ft.Row(controls=[root, content], expand=True, vertical_alignment=ft.CrossAxisAlignment.START))

if __name__ == "__main__":
    ft.app(target=main)
