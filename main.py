
def format_post(data):
    strengths_lines = []
    if data.get("Strength1"):
        strengths_lines.append(f"✅ {data['Strength1']}")
    if data.get("Strength2"):
        strengths_lines.append(f"✅ {data['Strength2']}")
    if data.get("Strength3"):
        strengths_lines.append(f"✅ {data['Strength3']}")
    return strengths_lines
