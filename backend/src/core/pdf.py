from __future__ import annotations

from typing import Iterable


def _escape_pdf_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf_bytes(title: str, lines: Iterable[str]) -> bytes:
    content_lines = [title] + [line for line in lines if line]
    text_ops = []
    y = 760
    for line in content_lines:
        text = _escape_pdf_text(line)
        text_ops.append(f"72 {y} Td ({text}) Tj")
        text_ops.append("T*")
        y -= 16
        if y < 72:
            break
    stream_content = "BT /F1 12 Tf " + " ".join(text_ops) + " ET"
    stream_bytes = stream_content.encode("utf-8")

    objects = []
    objects.append(b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n")
    objects.append(b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n")
    objects.append(
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
    )
    objects.append(
        b"4 0 obj << /Length "
        + str(len(stream_bytes)).encode("ascii")
        + b" >> stream\n"
        + stream_bytes
        + b"\nendstream endobj\n"
    )
    objects.append(b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n")

    header = b"%PDF-1.4\n"
    offsets = [0]
    current = len(header)
    for obj in objects:
        offsets.append(current)
        current += len(obj)

    xref_lines = ["xref", f"0 {len(offsets)}", "0000000000 65535 f "]
    for offset in offsets[1:]:
        xref_lines.append(f"{offset:010d} 00000 n ")
    xref_bytes = ("\n".join(xref_lines) + "\n").encode("ascii")
    trailer = (
        f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{current}\n%%EOF"
    ).encode("ascii")

    return header + b"".join(objects) + xref_bytes + trailer
