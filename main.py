# חלק מהקובץ main.py

# יצירת שורות חוזקה מתוך העמודה Strengths בפורמט ברור ונקי
strengths_lines = []
if strengths_src:
    for part in [p.strip() for p in strengths_src.replace("|", "\n").replace(";", "\n").split("\n") if p.strip()]:
        if not part.startswith("•") and not part.startswith("✔") and not part.startswith("-"):
            part = f"• {part}"
        strengths_lines.append(part)
