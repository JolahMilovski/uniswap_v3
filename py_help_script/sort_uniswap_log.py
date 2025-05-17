import re
from pathlib import Path
from collections import defaultdict

def extract_time_and_address(line):
    match = re.search(r'(\d{2}:\d{2}:\d{2}).*?(0x[a-fA-F0-9]{40})', line)
    if match:
        return match.group(1), match.group(2).lower()
    return None, None

def main():
    log_path = Path(__file__).parent / "log.md"
    if not log_path.exists():
        print("Файл log.md не найден в папке скрипта.")
        return

    pool_entries = defaultdict(list)
    total_graph_updates = 0
    total_graph_errors = 0
    total_failed = 0

    with open(log_path, "r", encoding="utf-8") as file:
        for line in file:
            if "[UNISWAP_EVENT_GRAPH_UPDATE]" in line:
                total_graph_updates += 1
            if "[UNISWAP_EVENT_POLLING_EVENT_ERROR]" in line:
                total_graph_errors += 1
            if "failed" in line.lower():
                total_failed += 1

            time, addr = extract_time_and_address(line)
            if addr:
                pool_entries[addr].append((time, line.rstrip()))

    sorted_output = []

    for addr in sorted(pool_entries):
        lines = sorted(pool_entries[addr], key=lambda x: x[0])
        sorted_output.extend(line for _, line in lines)
        sorted_output.append("")  # пустая строка между пулами

    sorted_output.append("---")
    sorted_output.append(f"Всего пулов: {len(pool_entries)}")
    sorted_output.append(f"[UNISWAP_EVENT_GRAPH_UPDATE] Обновлен пул: {total_graph_updates}")
    sorted_output.append(f"[UNISWAP_EVENT_POLLING_EVENT_ERROR] Ошибка обновления графа: {total_graph_errors}")
    sorted_output.append(f"Строк с 'failed': {total_failed}")

    output_path = log_path.parent / "sorted_log.md"
    with open(output_path, "w", encoding="utf-8") as out_file:
        out_file.write("\n".join(sorted_output))

    print(f"Готово. Отсортированный лог сохранён в {output_path}")

if __name__ == "__main__":
    main()
