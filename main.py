# main.py
# 1) pip install -U flet
# 2) python main.py

import math
import flet as ft

# ==============================
# TOKENS / CONSTANTES
# ==============================
W_COLLAPSED = 80
W_EXPANDED = 256
P_ROOT = 16     # p-4 do aside
P_ITEM = 12     # p-3 dos botões
ICON_SIZE = 24
RADIUS_ASIDE = 24
R_ITEM = 12     # rounded-lg
BAR_W = 6       # espessura da barrinha do item ativo
ANIM = ft.Animation(300, "easeInOut")

# Cores base (menu)
COLOR_ACTIVE_BG_LIGHT = "#EDE9FE"     # purple-100
COLOR_ACTIVE_BG_DARK  = "#2E1065"     # purple-950 aprox
COLOR_ACTIVE_BAR_LIGHT = "#8B5CF6"    # purple-500
COLOR_ACTIVE_BAR_DARK  = "#A78BFA"    # purple-300
TXT_ACTIVE_LIGHT = "#7C3AED"
TXT_ACTIVE_DARK  = "#E9D5FF"

# Paleta do dashboard
CHART_GREEN = "#10B981"   # GREEN-500
CHART_AMBER = "#F59E0B"   # amber-500
CHART_RED   = "#EF4444"   # red-500
CHART_GRAY_LIGHT = "#E5E7EB"  # gray-200
CHART_GRAY_DARK  = "#334155"  # slate-700

# ==============================
# MOCKS
# ==============================
DASHBOARD = {
    "total": 3,
    "valorTotal": "R$ 69.010",
    "vigentes": 1,
    "aVencer": 1,
}

ATAS = {
    "vigentes": [
        {
            "numero": "8555/5555",
            "vigencia": "23/12/2026",
            "objeto": "x",
            "fornecedor": "JIIJ",
            "situacao": "Vigente",
            "valorTotal": "R$ 10,00",
            "documentoSei": "56444.444444/4445-55",
            "itens": [{"descricao": "4", "quantidade": 1, "valorUnitario": "R$ 10,00", "subtotal": "R$ 10,00"}],
            "contatos": {"telefone": ["(55) 55555-5555"], "email": ["ssssss@gmail.com"]},
        },
    ],
    "vencidas": [
        {"numero": "4444/4444","vigencia": "04/01/2025","objeto": "s","fornecedor": "44444","situacao": "Vencida","valorTotal": "R$ 0,00","documentoSei": "","itens": [],"contatos": {"telefone": [], "email": []}},
        {"numero": "0102/0222","vigencia": "07/01/2024","objeto": "sabão","fornecedor": "mac","situacao": "Vencida","valorTotal": "R$ 0,00","documentoSei": "","itens": [],"contatos": {"telefone": [], "email": []}},
        {"numero": "0014/2024","vigencia": "31/12/2023","objeto": "Equipamentos de TI","fornecedor": "TechCorp Ltda","situacao": "Vencida","valorTotal": "R$ 0,00","documentoSei": "","itens": [],"contatos": {"telefone": [], "email": []}},
    ],
    "aVencer": [
        {"numero": "0000/1222","vigencia": "07/11/2025","objeto": "scanners","fornecedor": "EPSON","situacao": "A Vencer","valorTotal": "R$ 0,00","documentoSei": "","itens": [],"contatos": {"telefone": [], "email": []}},
    ],
}

# ==============================
# FUNÇÕES DE FORMATAÇÃO (Entrada)
# ==============================
def format_ata_number(val: str) -> str:
    if not val:
        return ""
    digits = "".join(ch for ch in val if ch.isdigit())
    if len(digits) > 8:
        digits = digits[:8]
    if len(digits) > 4:
        return f"{digits[:4]}/{digits[4:]}"
    return digits

def format_sei(val: str) -> str:
    if not val:
        return ""
    digits = "".join(ch for ch in val if ch.isdigit())
    digits = digits[:17]
    parts = []
    if len(digits) > 5:
        parts.append(digits[:5] + ".")
    if len(digits) > 11:
        parts.append(digits[5:11] + "/")
    if len(digits) > 15:
        parts.append(digits[11:15] + "-")
    parts.append(digits[15:17])
    return "".join(parts).strip(".-/")


# ==============================
# APP
# ==============================
def main(page: ft.Page):
    page.title = "Painel - Dashboard + Atas (Flet)"
    page.padding = 0
    page.theme_mode = ft.ThemeMode.LIGHT
    page.bgcolor = ft.Colors.GREY_100

    state = {"collapsed": True, "dark": False, "active": "dashboard"}

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

    # Conteúdo principal (vamos trocar essa coluna conforme a aba)
    content_col = ft.Column(expand=True, scroll=ft.ScrollMode.AUTO)
    content = ft.Container(expand=True, padding=20, content=content_col)

    # ---------------- helpers gerais ----------------
    def is_dark(): return state["dark"]
    def is_collapsed(): return state["collapsed"]

    def surface_bg():
        return ft.Colors.GREY_800 if is_dark() else ft.Colors.WHITE

    def text_color():
        return ft.Colors.GREY_200 if is_dark() else ft.Colors.GREY_900

    def text_muted():
        return ft.Colors.GREY_400 if is_dark() else ft.Colors.GREY_600

    def divider_color():
        return ft.Colors.with_opacity(0.12 if is_dark() else 0.08, ft.Colors.BLACK)

    def set_content(view):
        content_col.controls = [view]
        page.update()

    # ---------------- MENU (com barrinha integrada) ----------------
    def update_item_visual(key: str):
        ref = items[key]
        active = state["active"] == key

        ref["ink"].bgcolor = (COLOR_ACTIVE_BG_DARK if is_dark() else COLOR_ACTIVE_BG_LIGHT) if active else None
        ref["bar"].opacity = 1 if active else 0
        ref["bar"].bgcolor = COLOR_ACTIVE_BAR_DARK if is_dark() else COLOR_ACTIVE_BAR_LIGHT

        if is_collapsed():
            ref["text_box"].width = 0
            ref["text_box"].opacity = 0
            ref["text_box"].padding = 0
        else:
            ref["text_box"].width = W_EXPANDED - W_COLLAPSED - P_ITEM
            ref["text_box"].opacity = 1
            ref["text_box"].padding = ft.padding.only(right=8)

        base = ft.Colors.GREY_400 if is_dark() else ft.Colors.GREY_600
        if active:
            ref["icon"].color = TXT_ACTIVE_DARK if is_dark() else TXT_ACTIVE_LIGHT
            ref["text"].color = TXT_ACTIVE_DARK if is_dark() else TXT_ACTIVE_LIGHT
        else:
            ref["icon"].color = base
            ref["text"].color = base

    def set_active(key: str):
        state["active"] = key
        for k in items:
            update_item_visual(k)
        # troca a view
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
        if is_dark():
            page.theme_mode = ft.ThemeMode.DARK
            page.bgcolor = ft.Colors.GREY_900
            theme_icon.name = "light_mode"
            theme_text.value = "Modo Claro"
        else:
            page.theme_mode = ft.ThemeMode.LIGHT
            page.bgcolor = ft.Colors.GREY_100
            theme_icon.name = "dark_mode"
            theme_text.value = "Modo Escuro"
        update_theme_colors()

    def make_item(key: str, icon_name: str, label: str, active=False):
        icon = ft.Icon(icon_name, size=ICON_SIZE)
        txt  = ft.Text(label, size=13, weight=ft.FontWeight.W_600, no_wrap=True)

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
            bgcolor=COLOR_ACTIVE_BAR_LIGHT,
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

    # ---------------- THEME / COLORS UPDATE ----------------
    def update_theme_colors():
        root.bgcolor = ft.Colors.GREY_900 if is_dark() else ft.Colors.WHITE
        divider_top.bgcolor = divider_color()
        title_text.color = text_color()
        for k in items:
            update_item_visual(k)
        page.update()

    # ==============================
    # DASHBOARD VIEW
    # ==============================
    def StatCard(title: str, value: str, description: str, icon_name: str):
        return ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=ft.Colors.with_opacity(0.12, ft.Colors.BLACK)),
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
        sections = [
            ft.PieChartSection(1, title="", color=CHART_GREEN),
            ft.PieChartSection(1, title="", color=CHART_AMBER),
            ft.PieChartSection(1, title="", color=CHART_RED),
        ]
        chart = ft.PieChart(sections=sections, center_space_radius=45, sections_space=2, animate=ft.Animation(300, "easeOut"))
        legend = ft.Column(
            controls=[
                ft.Row([ft.Container(width=8, height=8, bgcolor=CHART_GREEN, border_radius=20), ft.Text("Vigentes: 1 (33,3%)", size=12, color=text_muted())], spacing=8),
                ft.Row([ft.Container(width=8, height=8, bgcolor=CHART_AMBER, border_radius=20), ft.Text("A Vencer: 1 (33,3%)", size=12, color=text_muted())], spacing=8),
                ft.Row([ft.Container(width=8, height=8, bgcolor=CHART_RED, border_radius=20), ft.Text("Vencidas: 1 (33,3%)", size=12, color=text_muted())], spacing=8),
            ],
            spacing=6,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        )
        return ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=ft.Colors.with_opacity(0.12, ft.Colors.BLACK)),
            content=ft.Column(
                controls=[ft.Text("Situação das Atas", size=16, weight=ft.FontWeight.W_600, color=text_color()),
                          ft.Container(content=chart, alignment=ft.alignment.center, padding=10),
                          ft.Container(content=legend, alignment=ft.alignment.center, padding=10)],
                spacing=10,
            ),
            col={"xs": 12, "lg": 6},
        )

    def Bars():
        months = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
        values = [0,0,0,0,0,0,0,0,10,45,60,0]
        groups = []
        for i, v in enumerate(values):
            color = (CHART_AMBER if i == 9 else CHART_RED if i == 10 else (CHART_GRAY_DARK if is_dark() else CHART_GRAY_LIGHT))
            groups.append(ft.BarChartGroup(x=i, bar_rods=[ft.BarChartRod(from_y=0, to_y=float(v), width=16, color=color, border_radius=4)]))
        chart = ft.BarChart(
            interactive=False, animate=ft.Animation(300, "easeOut"),
            max_y=70, min_y=0,
            bar_groups=groups,
            bottom_axis=ft.ChartAxis(labels=[ft.ChartAxisLabel(value=i, label=ft.Text(m, size=11, color=text_muted())) for i, m in enumerate(months)]),
            left_axis=ft.ChartAxis(show_labels=False),
            horizontal_grid_lines=ft.ChartGridLines(color=ft.Colors.with_opacity(0.08, ft.Colors.BLACK)),
        )
        return ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=ft.Colors.with_opacity(0.12, ft.Colors.BLACK)),
            content=ft.Column(controls=[ft.Text("Vencimentos por Mês", size=16, weight=ft.FontWeight.W_600, color=text_color()), chart], spacing=10),
            col={"xs": 12, "lg": 6},
        )

    def WarningCard():
        return ft.Container(
            bgcolor=ft.Colors.AMBER_100 if not is_dark() else ft.Colors.AMBER_900,
            border_radius=16,
            padding=20,
            shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=ft.Colors.with_opacity(0.12, ft.Colors.BLACK)),
            border=ft.border.only(left=ft.BorderSide(4, ft.Colors.AMBER)),
            content=ft.Column(
                controls=[
                    ft.Row([ft.Icon("warning", size=22, color=ft.Colors.AMBER), ft.Text("Atenção", weight=ft.FontWeight.W_600, color=text_color())], spacing=8),
                    ft.Text("Você possui 1 ata(s) vencendo em 59 dias ou menos.", size=12, color=text_muted()),
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
    def badge(text: str, variant: str):
        # variant: "green" | "amber" | "red"
        variant = (variant or "").lower()
        if variant == "green":
            bg, fg = (ft.Colors.GREEN_100, ft.Colors.GREEN_800)
            bgd, fgd = (ft.Colors.GREEN_900, ft.Colors.GREEN_100)
        elif variant == "amber":
            bg, fg = (ft.Colors.AMBER_100, ft.Colors.AMBER_900)
            bgd, fgd = (ft.Colors.AMBER_900, ft.Colors.AMBER_100)
        else:
            bg, fg = (ft.Colors.RED_100, ft.Colors.RED_800)
            bgd, fgd = (ft.Colors.RED_900, ft.Colors.RED_100)
        return ft.Container(
            padding=ft.padding.symmetric(2, 10),
            bgcolor=bgd if is_dark() else bg,
            border_radius=999,
            content=ft.Text(text, size=11, color=fgd if is_dark() else fg, weight=ft.FontWeight.W_600)
        )

    def situacao_to_variant(s: str) -> str:
        s = (s or "").lower()
        if s == "vigente":
            return "green"
        if s == "a vencer":
            return "amber"
        return "red"

    # ---------- List-Card (igual ao print) ----------
# ---------- List-Card (com colunas + separadores + títulos centralizados) ----------
    def AtasSectionCard(title: str, icon_name: str, data: list[dict]):
        def vsep(h=28):
            return ft.Container(width=1, height=h, bgcolor=divider_color())

        # Cabeçalho
        header = ft.Row(
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=10,
            controls=[
                ft.Container(
                    width=28, height=28, border_radius=999,
                    bgcolor=ft.Colors.with_opacity(0.08, ft.Colors.BLACK),
                    alignment=ft.alignment.center,
                    content=ft.Icon(icon_name, size=18, color=text_muted()),
                ),
                ft.Text(title, size=16, weight=ft.FontWeight.W_600, color=text_color()),
            ],
        )

        # Cabeçalho das colunas (centralizado)
        cols_head = ft.Container(
            padding=ft.padding.symmetric(vertical=10, horizontal=12),
            bgcolor=ft.Colors.with_opacity(0.03, ft.Colors.BLACK),
            border_radius=8,
            content=ft.Row(
                spacing=8,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(ft.Text("NÚMERO", size=11, color=text_muted()), expand=2, alignment=ft.alignment.center),
                    vsep(),
                    ft.Container(ft.Text("VIGÊNCIA", size=11, color=text_muted()), expand=2, alignment=ft.alignment.center),
                    vsep(),
                    ft.Container(ft.Text("OBJETO", size=11, color=text_muted()), expand=3, alignment=ft.alignment.center),
                    vsep(),
                    ft.Container(ft.Text("FORNECEDOR", size=11, color=text_muted()), expand=3, alignment=ft.alignment.center),
                    vsep(),
                    ft.Container(ft.Text("SITUAÇÃO", size=11, color=text_muted()), expand=2, alignment=ft.alignment.center),
                    vsep(),
                    ft.Container(
                        ft.Text("AÇÕES", size=11, color=text_muted()),
                        width=140, alignment=ft.alignment.center
                    ),
                ],
            )
        )

        # Linhas (dados)
        rows_ui = []
        for ata in data:
            rows_ui.append(
                ft.Container(
                    padding=ft.padding.symmetric(vertical=14, horizontal=12),
                    content=ft.Row(
                        spacing=8,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                        controls=[
                            ft.Container(ft.Text(ata["numero"], color=text_color()), expand=2, alignment=ft.alignment.center),
                            vsep(24),
                            ft.Container(ft.Text(ata["vigencia"], color=text_muted()), expand=2, alignment=ft.alignment.center),
                            vsep(24),
                            ft.Container(ft.Text(ata["objeto"], color=text_muted()), expand=3, alignment=ft.alignment.center),
                            vsep(24),
                            ft.Container(ft.Text(ata["fornecedor"], color=text_muted()), expand=3, alignment=ft.alignment.center),
                            vsep(24),
                            ft.Container(
                                badge(ata["situacao"], situacao_to_variant(ata["situacao"])),
                                expand=2, alignment=ft.alignment.center,
                            ),
                            vsep(24),
                            ft.Container(
                                alignment=ft.alignment.center,
                                width=140,
                                content=ft.Row(
                                    spacing=6,
                                    alignment=ft.MainAxisAlignment.CENTER,
                                    controls=[
                                        ft.IconButton(icon="visibility", tooltip="Ver",   on_click=lambda e, a=ata: show_ata_details(a)),
                                        ft.IconButton(icon="edit",       tooltip="Editar", on_click=lambda e, a=ata: show_ata_edit(a)),
                                        ft.IconButton(icon="delete",     tooltip="Excluir", icon_color=ft.Colors.RED_400),
                                    ],
                                ),
                            ),
                        ],
                    ),
                )
            )

        return ft.Container(
            col=12,
            bgcolor=surface_bg(),
            border_radius=16,
            padding=16,
            shadow=ft.BoxShadow(
                blur_radius=16, spread_radius=1,
                color=ft.Colors.with_opacity(0.10, ft.Colors.BLACK),
            ),
            content=ft.Column(
                spacing=10,
                controls=[header, cols_head, *rows_ui] if data else [header, ft.Text("Nenhum registro.", color=text_muted())],
            ),
        )



    def AtasPage():
        # barra superior (busca + ações)
        search = ft.TextField(
            hint_text="Buscar atas...",
            prefix_icon=ft.Icons.SEARCH,
            border_radius=999,
            bgcolor=ft.Colors.with_opacity(0.04, ft.Colors.BLACK),
            expand=True,
        )

        actions = ft.Row(
            controls=[
                ft.OutlinedButton("Filtrar", icon="filter_list"),
                ft.OutlinedButton("Ordenar", icon="sort"),
                ft.FilledButton("Nova Ata", icon="add"),
            ],
            spacing=8,
        )

        top = ft.Container(
            bgcolor=surface_bg(),
            border_radius=16,
            padding=16,
            shadow=ft.BoxShadow(
                blur_radius=12, spread_radius=1,
                color=ft.Colors.with_opacity(0.08, ft.Colors.BLACK),
            ),
            content=ft.ResponsiveRow(
                columns=12,
                controls=[
                    ft.Container(content=ft.Row([ft.Icon("search", color=text_muted()), search], spacing=8), col={"xs":12, "md":7}),
                    ft.Container(content=actions, col={"xs":12, "md":5}, alignment=ft.alignment.center_right),
                ],
            ),
        )

        # Cards empilhados (como no print)
        grid = ft.ResponsiveRow(
            columns=12,
            spacing=16,
            run_spacing=16,
            controls=[
                ft.Container(content=top, col=12),
                AtasSectionCard("Atas Vigentes", "check_circle", ATAS["vigentes"]),
                AtasSectionCard("Atas Vencidas", "cancel", ATAS["vencidas"]),
                AtasSectionCard("Atas a Vencer", "schedule", ATAS["aVencer"]),
            ],
        )

        return grid

    # --------- Detalhes ---------
    def show_ata_details(ata: dict):
        title = ft.Text("Ata de Registro de Preços", size=20, weight=ft.FontWeight.W_700, color=text_color())
        subtitle = ft.Text(f"Nº {ata['numero']}", size=13, color=text_muted())
        header = ft.Row(
            controls=[
                ft.Column(controls=[title, subtitle], spacing=2, expand=True),
                ft.Row(controls=[
                    ft.OutlinedButton("Voltar", icon="arrow_back", on_click=lambda e: set_content(AtasPage())),
                    ft.FilledButton("Editar", icon="edit", on_click=lambda e, a=ata: show_ata_edit(a)),
                ], spacing=8)
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )

        # Cards: Dados gerais / Fornecedor
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
                        ft.Row([ft.Icon("call", size=16, color=text_muted()), ft.Text(", ".join(ata['contatos'].get('telefone') or ['—']), color=text_muted())]),
                        ft.Row([ft.Icon("mail", size=16, color=text_muted()), ft.Text(", ".join(ata['contatos'].get('email') or ['—']), color=text_muted())]),
                    ], spacing=6)
                ], spacing=10
            ),
        )
        grid_top = ft.ResponsiveRow(columns=12, spacing=16, run_spacing=16, controls=[
            ft.Container(content=dados, col={"xs":12, "lg":6}),
            ft.Container(content=forn,  col={"xs":12, "lg":6}),
        ])

        # Tabela de itens
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

    # --------- Edição ---------
    def show_ata_edit(ata: dict):
        # Estado local (referências)
        numero = ft.TextField(label="Número da Ata", value=ata.get("numero",""))
        documento_sei = ft.TextField(label="Documento SEI", value=ata.get("documentoSei",""))
        data_vigencia = ft.TextField(label="Data de Vigência", value=ata.get("vigencia",""))
        objeto = ft.TextField(label="Objeto", value=ata.get("objeto",""))
        fornecedor = ft.TextField(label="Fornecedor", value=ata.get("fornecedor",""))

        # máscaras simples
        def on_num_change(e):
            e.control.value = format_ata_number(e.control.value)
            page.update()
        numero.on_change = on_num_change

        def on_sei_change(e):
            e.control.value = format_sei(e.control.value)
            page.update()
        documento_sei.on_change = on_sei_change

        # contatos
        tels = [ft.TextField(label=f"Telefone {i+1}", value=v, prefix_icon=ft.Icons.PHONE) for i, v in enumerate(ata["contatos"].get("telefone") or [])]
        emails = [ft.TextField(label=f"E-mail {i+1}", value=v, prefix_icon=ft.Icons.MAIL) for i, v in enumerate(ata["contatos"].get("email") or [])]

        def add_tel(e):
            tels.append(ft.TextField(label=f"Telefone {len(tels)+1}", value="", prefix_icon=ft.Icons.PHONE))
            refresh()
        def add_email(e):
            emails.append(ft.TextField(label=f"E-mail {len(emails)+1}", value="", prefix_icon=ft.Icons.MAIL))
            refresh()

        # itens
        itens = ata.get("itens")[:] if ata.get("itens") else [{"descricao":"","quantidade":"","valorUnitario":""}]
        itens_fields = []
        def build_item_row(idx, item):
            desc = ft.TextField(label="Descrição", value=item.get("descricao",""), expand=True)
            qtd  = ft.TextField(label="Qtd.", value=str(item.get("quantidade","")), width=80)
            vu   = ft.TextField(label="Valor Unit.", value=item.get("valorUnitario",""), width=120)
            del_btn = ft.IconButton(icon="delete", tooltip="Excluir", on_click=lambda e, i=idx: confirm_delete("item", i))
            return ft.Row([desc, qtd, vu, del_btn], spacing=8)

        for i, it in enumerate(itens):
            itens_fields.append(build_item_row(i, it))

        def add_item(e):
            itens.append({"descricao":"","quantidade":"","valorUnitario":""})
            itens_fields.append(build_item_row(len(itens_fields), itens[-1]))
            refresh()

        # modal confirmação
        dlg = ft.AlertDialog(modal=True, title=ft.Text("Confirmar exclusão"), content=ft.Text("Tem certeza de que deseja excluir este item?"), actions=[])
        delete_ctx = {"type": None, "index": None}

        def confirm_delete(kind, idx):
            delete_ctx["type"]=kind; delete_ctx["index"]=idx
            dlg.actions = [
                ft.TextButton("Cancelar", on_click=lambda e: page.close(dlg)),
                ft.FilledButton("Excluir", icon="delete", on_click=do_delete)
            ]
            page.open(dlg)

        def do_delete(e):
            if delete_ctx["type"] == "telefone":
                if 0 <= delete_ctx["index"] < len(tels):
                    dels = delete_ctx["index"]
                    tels.pop(dels)
                    for i, t in enumerate(tels): t.label = f"Telefone {i+1}"
            elif delete_ctx["type"] == "email":
                if 0 <= delete_ctx["index"] < len(emails):
                    dels = delete_ctx["index"]; emails.pop(dels)
                    for i, t in enumerate(emails): t.label = f"E-mail {i+1}"
            elif delete_ctx["type"] == "item":
                if 0 <= delete_ctx["index"] < len(itens_fields):
                    idx = delete_ctx["index"]
                    itens_fields.pop(idx)
            page.close(dlg)
            refresh()

        def refresh():
            contacts_col.controls = [
                ft.Row([ft.Text("Contatos", size=16, weight=ft.FontWeight.W_600, color=text_color()),
                        ft.TextButton("Adicionar telefone", icon="add", on_click=add_tel),
                        ft.TextButton("Adicionar e-mail", icon="add", on_click=add_email)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Column([ft.Row([t, ft.IconButton(icon="delete", tooltip="Excluir", on_click=lambda e, i=i: confirm_delete("telefone", i))], spacing=8) for i, t in enumerate(tels)], spacing=8),
                ft.Column([ft.Row([m, ft.IconButton(icon="delete", tooltip="Excluir", on_click=lambda e, i=i: confirm_delete("email", i))], spacing=8) for i, m in enumerate(emails)], spacing=8),
            ]
            itens_col.controls = [
                ft.Row([ft.Text("Itens", size=16, weight=ft.FontWeight.W_600, color=text_color()),
                        ft.TextButton("Adicionar", icon="add", on_click=add_item)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Column(itens_fields, spacing=8)
            ]
            page.update()

        # layout cabeçalho
        header = ft.Row(
            controls=[
                ft.Column([ft.Text("Ata de Registro de Preços", size=20, weight=ft.FontWeight.W_700, color=text_color()),
                           ft.Text("Editar Ata", size=13, color=text_muted())], spacing=2, expand=True),
                ft.Row([
                    ft.OutlinedButton("Voltar", icon="arrow_back", on_click=lambda e: set_content(AtasPage())),
                    ft.FilledButton("Salvar", icon="save", on_click=lambda e: (show_snack("Ata salva com sucesso!"), set_content(AtasPage()))),
                ], spacing=8),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN
        )

        # cards esquerda
        dados_gerais = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=ft.Column(
                controls=[
                    ft.Text("Dados Gerais", size=16, weight=ft.FontWeight.W_600, color=text_color()),
                    numero, documento_sei, data_vigencia, objeto, fornecedor
                ],
                spacing=10,
            ),
        )

        contacts_col = ft.Column(spacing=10)
        contatos_card = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=contacts_col
        )

        grid_top = ft.ResponsiveRow(
            columns=12, spacing=16, run_spacing=16,
            controls=[ft.Container(content=dados_gerais, col={"xs":12,"lg":6}),
                      ft.Container(content=contatos_card, col={"xs":12,"lg":6})]
        )

        itens_col = ft.Column(spacing=10)
        itens_card = ft.Container(
            bgcolor=surface_bg(), border_radius=16, padding=16,
            content=itens_col
        )

        view = ft.Column(controls=[header, grid_top, itens_card], spacing=16)
        set_content(view)
        refresh()

    def show_snack(msg: str):
        page.snack_bar = ft.SnackBar(ft.Text(msg))
        page.snack_bar.open = True
        page.update()

    # ==============================
    # PÁGINAS SIMPLES (placeholders)
    # ==============================
    def SimplePage(title: str, subtitle: str):
        return ft.Column(controls=[
            ft.Container(
                bgcolor=surface_bg(), border_radius=16, padding=16,
                content=ft.Column(controls=[ft.Text(title, size=18, weight=ft.FontWeight.W_600, color=text_color()),
                                            ft.Text(subtitle, color=text_muted())], spacing=6))
        ])

    # ==============================
    # ASIDE (MENU)
    # ==============================
    top_logo = ft.Container(height=56, alignment=ft.alignment.center,
                            content=ft.Icon("diamond", size=ICON_SIZE, color=ft.Colors.GREY_500),
                            padding=ft.padding.only(top=8, bottom=8))
    menu_icon = ft.Icon("menu", size=ICON_SIZE, rotate=ft.Rotate(0, alignment=ft.alignment.center), animate_rotation=ANIM)
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

    theme_icon = ft.Icon("dark_mode", size=ICON_SIZE, color=ft.Colors.GREY_600)
    theme_text  = ft.Text("Modo Escuro", size=13, weight=ft.FontWeight.W_600, no_wrap=True, opacity=0)
    theme_text_box = ft.Container(
        alignment=ft.alignment.center_left, content=theme_text,
        width=0, animate=ANIM, animate_opacity=300, clip_behavior=ft.ClipBehavior.HARD_EDGE,
    )
    theme_btn = ft.Container(
        content=ft.Row(controls=[theme_icon, theme_text_box], spacing=12,
                       alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER),
        padding=P_ITEM, border_radius=R_ITEM, on_click=toggle_theme, tooltip="Alternar tema",
    )

    root = ft.Container(
        width=W_COLLAPSED, bgcolor=ft.Colors.WHITE,
        border_radius=ft.border_radius.only(top_right=RADIUS_ASIDE, bottom_right=RADIUS_ASIDE),
        padding=P_ROOT,
        content=ft.Column(
            controls=[top_logo, header_btn, divider_top, ft.Container(height=8), nav, ft.Container(height=8), ft.Container(height=1), theme_btn],
            expand=True, spacing=0,
        ),
        animate=ANIM,
        shadow=ft.BoxShadow(blur_radius=18, spread_radius=1, color=ft.Colors.with_opacity(0.15, ft.Colors.BLACK)),
    )

    # Estado inicial & conteúdo inicial
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
    update_theme_colors()

    # Layout
    page.add(ft.Row(controls=[root, content], expand=True, vertical_alignment=ft.CrossAxisAlignment.START))

if __name__ == "__main__":
    ft.app(target=main)
