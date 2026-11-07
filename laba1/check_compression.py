import os
import re

ALGOS = ["none", "snappy", "lz4", "zstd"]


def get_topic_log_bytes(kafka_logs_dir, topic_prefix):
    pat = re.compile(rf"^{re.escape(topic_prefix)}-\d+$")
    total_bytes = 0
    part_count = 0
    if not os.path.exists(kafka_logs_dir):
        return 0, 0
    for entry in sorted(os.listdir(kafka_logs_dir)):
        full_dir = os.path.join(kafka_logs_dir, entry)
        if os.path.isdir(full_dir) and pat.match(entry):
            part_count += 1
            for fname in os.listdir(full_dir):
                if fname.endswith(".log"):
                    total_bytes += os.path.getsize(os.path.join(full_dir, fname))
    return total_bytes, part_count


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    kafka_logs_dir = os.path.join(repo_root, "kafka-logs")

    base_bytes, _ = get_topic_log_bytes(kafka_logs_dir, "smart-comp-none")
    print("Фактичний розмір сегментів .log у kafka-logs та Compression Ratio:")
    print(f"{'Алгоритм':<10} | {'Партицій':<8} | {'Розмір (байт)':<15} | {'Розмір (КБ)':<12} | {'Compression Ratio':<18} | {'Кратність'}")
    print("-" * 86)

    for alg in ALGOS:
        size_b, n_parts = get_topic_log_bytes(kafka_logs_dir, f"smart-comp-{alg}")
        size_kb = size_b / 1024.0
        if base_bytes > 0 and alg != "none" and size_b > 0:
            saved_pct = (1.0 - (size_b / base_bytes)) * 100.0
            ratio_x = base_bytes / size_b
        else:
            saved_pct = 0.0
            ratio_x = 1.0
        print(f"{alg:<10} | {n_parts:<8d} | {size_b:<15,d} | {size_kb:<12.2f} | {saved_pct:>13.2f}%     | {ratio_x:.2f}x")


if __name__ == "__main__":
    main()
