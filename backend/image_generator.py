import math
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Polygon, Circle
import os

def get_fear_greed_category(value):
    if value is None: return "Unknown"
    if value <= 25: return "Extreme Fear"
    if value <= 45: return "Fear"
    if value <= 55: return "Neutral"
    if value <= 75: return "Greed"
    return "Extreme Greed"

def generate_fear_greed_chart(data):
    """
    Generates the Fear & Greed Index gauge chart and saves it as a PNG image.
    The data structure is expected to be similar to the example provided by the user.
    """
    output_path = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'fear_and_greed_gauge.png')

    # New, more detailed color scheme
    status_colors = {
        "Extreme Fear":  ("#cc6600", "#994c00"), # bg, border
        "Fear":          ("#f6a35c", "#cc6600"),
        "Neutral":       ("#bfbfbf", "#666666"),
        "Greed":         ("#66cc99", "#006633"),
        "Extreme Greed": ("#006633", "#004c24")
    }

    # ===== ゲージ描画 =====
    value = data["center_value"]
    current_category = get_fear_greed_category(value)

    # Define segments with their value ranges, angles, and category name
    segments = [
        {"label": "EXTREME FEAR",  "category": "Extreme Fear",  "angle": (135, 180)},
        {"label": "FEAR",          "category": "Fear",          "angle": (99, 135)},
        {"label": "NEUTRAL",       "category": "Neutral",       "angle": (81, 99)},
        {"label": "GREED",         "category": "Greed",         "angle": (45, 81)},
        {"label": "EXTREME GREED", "category": "Extreme Greed", "angle": (0, 45)}
    ]
    start_angle = 180
    end_angle = 0
    radius_outer = 1.0
    radius_inner = 0.6

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={'aspect':'equal'})
    ax.set_xlim(-1.5, 1.5)
    ax.set_ylim(-1.3, 1.5)
    ax.axis('off')

    for segment in segments:
        a2, a1 = segment["angle"]  # start and end angles for the wedge

        is_active = (current_category == segment["category"])

        if is_active:
            # Use the new color scheme for the active segment
            face, _ = status_colors.get(current_category, ("#e0e0e0", "black"))
            edge = 'black'
            lw = 1.5
        else:
            face = '#f0f0f0'
            edge = '#d3d3d3'
            lw = 1.0

        wedge = Wedge((0,0), radius_outer, a2, a1, width=radius_outer-radius_inner,
                      facecolor=face, edgecolor=edge, linewidth=lw, zorder=1)
        ax.add_patch(wedge)

        mid_angle = math.radians((a1 + a2) / 2)
        lx = (radius_outer + 0.15) * math.cos(mid_angle)
        ly = (radius_outer + 0.15) * math.sin(mid_angle)
        ax.text(lx, ly, segment["label"], ha='center', va='center', fontsize=11, fontweight='bold', color='#555555')

    # 目盛り（数字と点、5刻み）
    for pct in range(0, 101, 5):
        ang = math.radians(start_angle - (pct/100)*(start_angle-end_angle))
        r_text = radius_inner - 0.1
        x_text = r_text * math.cos(ang)
        y_text = r_text * math.sin(ang)
        if pct % 25 == 0:
            ax.text(x_text, y_text, str(pct), ha='center', va='center', fontsize=9, color='#333333')
        else:
            ax.plot([x_text], [y_text], marker='.', markersize=4, color='grey', zorder=2)

    # 針
    needle_angle = math.radians(start_angle - (value/100)*(start_angle-end_angle))
    needle_length = radius_outer - 0.05
    w = 0.02
    dx = w * math.cos(needle_angle + math.pi/2)
    dy = w * math.sin(needle_angle + math.pi/2)
    x_tip = needle_length * math.cos(needle_angle)
    y_tip = needle_length * math.sin(needle_angle)
    poly_coords = [(-dx, -dy*2), (x_tip, y_tip), (dx, -dy*2)]
    needle = Polygon(poly_coords, closed=True, facecolor='black', edgecolor='black', zorder=4)
    ax.add_patch(needle)

    # 中央の数字 (枠線を削除)
    center_pivot = Circle((0,0), 0.15, facecolor='white', zorder=5) # edgecolor and linewidth removed
    ax.add_patch(center_pivot)
    ax.text(0, 0, str(value), fontsize=32, fontweight='bold', ha='center', va='center', zorder=6)

    # ===== 下部情報エリア (縦一列に修正) =====
    history = data["history"]
    history_keys = ["previous_close", "week_ago", "month_ago", "year_ago"]

    start_y = -0.25
    y_step = -0.2
    x_label = -1.4
    x_status = 0.0
    x_circle = 1.0

    for i, key in enumerate(history_keys):
        if key not in history:
            continue
        item = history[key]
        label = item["label"]
        status = item["status"]
        val = item["value"]
        # Use the new color scheme for historical data circles
        bg, border = status_colors.get(status, ("#cccccc", "#666666"))

        current_y = start_y + i * y_step

        ax.text(x_label, current_y, label, ha='left', va='center', fontsize=10, color='grey')
        ax.text(x_status, current_y, status, ha='left', va='center', fontsize=10, fontweight='bold')

        circle = Circle((x_circle, current_y), 0.1, facecolor=bg, edgecolor=border, linewidth=1.0, zorder=3)
        ax.add_patch(circle)
        ax.text(x_circle, current_y, str(val), ha='center', va='center', fontsize=10, fontweight='bold', color='black')

        if i < len(history_keys) - 1:
            line_y = current_y + y_step / 2
            ax.plot([x_label, x_circle + 0.3], [line_y, line_y], color='#e0e0e0', linestyle='dotted', linewidth=1)

    plt.savefig(output_path, bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
