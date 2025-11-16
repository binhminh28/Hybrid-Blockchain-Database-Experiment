import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.ticker as ticker
from matplotlib.ticker import ScalarFormatter

print("=============================================")
print("  Trình vẽ biểu đồ Hybrid Blockchain (Tất cả Figure)")
print("=============================================")

# --- PHẦN 1: CẤU HÌNH VÀ HÀM HỖ TRỢ ---

# Thư mục chính chứa tất cả các thư mục log
LOG_DIRECTORY = "logs"

# Ánh xạ tên thư mục sang tên hệ thống (key) và loại thí nghiệm (value)
DIR_REGEX_MAP = {
    re.compile(r'logs-clients-veritas-kafka'): ("Veritas (Kafka)", "clients"),
    re.compile(r'logs-clients-veritas-tendermint'): ("Veritas (TM)", "clients"),
    re.compile(r'logs-clients-bigchaindb'): ("BigchainDB", "clients"),
    re.compile(r'logs-clients-bigchaindb-pv'): ("BigchainDB (PV)", "clients"),
    re.compile(r'logs-clients-blockchaindb'): ("BlockchainDB", "clients"),
    
    re.compile(r'logs-nodes-veritas-kafka'): ("Veritas (Kafka)", "nodes"),
    re.compile(r'logs-nodes-veritas-tendermint'): ("Veritas (TM)", "nodes"),
    re.compile(r'logs-nodes-bigchaindb'): ("BigchainDB", "nodes"),
    re.compile(r'logs-nodes-bigchaindb-pv'): ("BigchainDB (PV)", "nodes"),
    re.compile(r'logs-nodes-blockchaindb'): ("BlockchainDB", "nodes"),
    
    re.compile(r'logs-distribution-veritas-kafka'): ("Veritas (Kafka)", "distribution"),
    re.compile(r'logs-distribution-veritas-tendermint'): ("Veritas (TM)", "distribution"),
    re.compile(r'logs-distribution-bigchaindb'): ("BigchainDB", "distribution"),
    re.compile(r'logs-distribution-bigchaindb-pv'): ("BigchainDB (PV)", "distribution"),
    re.compile(r'logs-distribution-blockchaindb'): ("BlockchainDB", "distribution"),

    re.compile(r'logs-workload-veritas-kafka'): ("Veritas (Kafka)", "workload"),
    re.compile(r'logs-workload-veritas-tendermint'): ("Veritas (TM)", "workload"),
    re.compile(r'logs-workload-bigchaindb'): ("BigchainDB", "workload"),
    re.compile(r'logs-workload-bigchaindb-pv'): ("BigchainDB (PV)", "workload"),
    re.compile(r'logs-workload-blockchaindb'): ("BlockchainDB", "workload"),
    
    re.compile(r'logs-database-veritas-kafka'): ("Veritas (Kafka) - RediSQL", "database"), 
    re.compile(r'logs-workload-veritas-kafka.*'): ("Veritas (Kafka) - Redis", "database"), 

    re.compile(r'logs-txsizes-veritas-kafka'): ("Veritas (Kafka)", "txsize"),
    re.compile(r'logs-txsizes-veritas-tendermint'): ("Veritas (TM)", "txsize"),
    re.compile(r'logs-txsizes-bigchaindb'): ("BigchainDB", "txsize"),
    re.compile(r'logs-txsizes-bigchaindb-pv'): ("BigchainDB (PV)", "txsize"),
    re.compile(r'logs-txsizes-blockchaindb'): ("BlockchainDB", "txsize"),

    re.compile(r'logs-txdelay-veritas-kafka'): ("Veritas (Kafka)", "txdelay"),
    re.compile(r'logs-txdelay-veritas-tendermint'): ("Veritas (TM)", "txdelay"),
    re.compile(r'logs-txdelay-bigchaindb'): ("BigchainDB", "txdelay"),
    re.compile(r'logs-txdelay-bigchaindb-pv'): ("BigchainDB (PV)", "txdelay"),
    re.compile(r'logs-txdelay-blockchaindb'): ("BlockchainDB", "txdelay"),

    re.compile(r'logs-networking-veritas-kafka'): ("Veritas (Kafka)", "networking"),
    re.compile(r'logs-networking-veritas-tendermint'): ("Veritas (TM)", "networking"),
    re.compile(r'logs-networking-bigchaindb'): ("BigchainDB", "networking"),
    re.compile(r'logs-networking-bigchaindb-pv'): ("BigchainDB (PV)", "networking"),
    re.compile(r'logs-networking-blockchaindb'): ("BlockchainDB", "networking"),
    
    re.compile(r'logs-blksize-veritas-kafka'): ("Veritas (Kafka)", "blksize"),
    re.compile(r'logs-blksize-veritas-tendermint'): ("Veritas (TM)", "blksize"),
    re.compile(r'logs-blksize-blockchaindb'): ("BlockchainDB", "blksize"),
}

# Ánh xạ tên file (hoặc một phần) sang giá trị trên trục X
DISTRIBUTION_MAP = {"uniform": "Uniform", "latest": "Latest", "zipfian": "Zipfian"}
WORKLOAD_MAP = {"workloada": "Workload A", "workloadb": "Workload B", "workloadc": "Workload C"}
DATABASE_MAP = {"veritas-redisql-workloada": "Workload A", "veritas-redisql-workloadb": "Workload B", "veritas-redisql-workloadc": "Workload C",
                "veritas-kafka-workloada": "Workload A", "veritas-kafka-workloadb": "Workload B", "veritas-kafka-workloadc": "Workload C"}


def parse_log_file(filepath):
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            throughput_match = re.search(r'Throughput.*: (\d+) req/s', content)
            latency_match = re.search(r'Average latency: ([\d.]+) ms', content)
            if throughput_match and latency_match:
                return int(throughput_match.group(1)), float(latency_match.group(1))
    except Exception as e:
        print(f"    [Lỗi đọc file] {filepath}: {e}")
    return None, None

def parse_kafka_counters(filepath):
    total_read_count, max_write_count = 0, 0
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if "rdkafka" in line:
                    parts = line.split()
                    if len(parts) >= 5:
                        try:
                            total_read_count += int(parts[3])
                            max_write_count = max(max_write_count, int(parts[4]))
                        except (ValueError, IndexError): continue
    except Exception as e:
        print(f"    [Lỗi đọc file] {filepath}: {e}")
        return None, None
    if total_read_count == 0 and max_write_count == 0: return None, None
    return total_read_count, max_write_count

def convert_size_to_kb(num_str, unit_str):
    try:
        num = float(num_str)
        unit = unit_str.lower()
        if unit == 'b':
            return num / 1024.0
        elif unit == 'kb':
            return num
    except Exception:
        return None
    return None

# --- PHẦN 2: HÀM THU THẬP DỮ LIỆU ---

def collect_all_data():
    data_fig5_clients, data_fig7_nodes, data_fig8_kafka_ops = [], [], []
    data_fig9_dist, data_fig10_workload, data_fig11_db = [], [], []
    data_fig12_blksize, data_fig13_txsize, data_fig14_txdelay, data_fig15_16_net = [], [], [], []
    
    print(f"Bắt đầu quét thư mục log chính: '{LOG_DIRECTORY}'...")
    if not os.path.isdir(LOG_DIRECTORY):
        print(f"LỖI: Không tìm thấy thư mục '{LOG_DIRECTORY}'. Vui lòng tạo thư mục và đặt log vào đó.")
        return {}

    for dir_name in os.listdir(LOG_DIRECTORY):
        dir_path = os.path.join(LOG_DIRECTORY, dir_name)
        if not os.path.isdir(dir_path): continue
        system_name, experiment_type = None, None
        
        for regex, (sys_name, exp_type) in DIR_REGEX_MAP.items():
            if regex.search(dir_name):
                system_name, experiment_type = sys_name, exp_type
                break
        if not system_name: continue
            
        print(f"\n+ Đang xử lý: {dir_name} (Hệ thống: {system_name}, Thí nghiệm: {experiment_type})")
        
        if experiment_type == "clients":
            file_regex = re.compile(r'.*-(\d+)\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    clients = int(match.group(1))
                    tps, lat = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig5_clients.append({"system": system_name, "clients": clients, "throughput": tps, "latency": lat})
        
        elif experiment_type == "nodes":
            file_regex = re.compile(r'.*-nodes-(\d+)\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    nodes = int(match.group(1))
                    tps, lat = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig7_nodes.append({"system": system_name, "nodes": nodes, "throughput": tps, "latency": lat})
            
            if system_name == "Veritas (Kafka)":
                log_subdir_regex = re.compile(r'veritas-nodes-(\d+)-logs$')
                for f in os.listdir(dir_path):
                    if os.path.isdir(os.path.join(dir_path, f)):
                        match = log_subdir_regex.match(f)
                        if match:
                            nodes = int(match.group(1))
                            counters_file = os.path.join(dir_path, f, "kafka-counters.log")
                            if os.path.exists(counters_file):
                                reads, writes = parse_kafka_counters(counters_file)
                                if reads:
                                    data_fig8_kafka_ops.append({"Nodes": nodes, "Operation Type": "Read Count", "Operations": reads})
                                    data_fig8_kafka_ops.append({"Nodes": nodes, "Operation Type": "Write Count", "Operations": writes})
                            else: print(f"    [Cảnh báo Fig 8] Không tìm thấy 'kafka-counters.log' trong {f}")
                            
        elif experiment_type == "distribution":
            file_regex = re.compile(r'.*-(uniform|latest|zipfian)\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    dist_name = DISTRIBUTION_MAP.get(match.group(1), match.group(1))
                    tps, _ = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig9_dist.append({"System": system_name, "Distribution": dist_name, "Throughput (TPS)": tps})

        elif experiment_type == "workload":
            file_regex = re.compile(r'.*-workload(a|b|c)\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    workload_name = WORKLOAD_MAP.get(f"workload{match.group(1)}", match.group(1))
                    tps, _ = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig10_workload.append({"System": system_name, "Workload": workload_name, "Throughput (TPS)": tps})

        elif experiment_type == "database":
            file_regex = re.compile(r'.*-(workload(a|b|c)|redisql-workload(a|b|c))\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    workload_key = f"workload{match.group(1)[-1]}"
                    workload_name = WORKLOAD_MAP.get(workload_key, workload_key)
                    db_name = "Veritas (Kafka) - Redis" if "kafka" in system_name else "Veritas (Kafka) - RediSQL"
                    tps, _ = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig11_db.append({"Database": db_name, "Workload": workload_name, "Throughput (TPS)": tps})
        
        elif experiment_type == "blksize":
            file_regex = re.compile(r'.*-blksize-(\d+)\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    blksize = int(match.group(1))
                    tps, _ = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig12_blksize.append({"system": system_name, "blksize": blksize, "throughput": tps})
                        
        elif experiment_type == "txsize":
            file_regex = re.compile(r'.*-txsize-([\d.]+)([kK]?[Bb])\.txt$')
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    size_kb = convert_size_to_kb(match.group(1), match.group(2))
                    if size_kb is not None:
                        tps, _ = parse_log_file(os.path.join(dir_path, f))
                        if tps:
                            label = f"{match.group(1)}{match.group(2).upper()}"
                            data_fig13_txsize.append({"system": system_name, "size_kb": size_kb, "throughput": tps, "size_label": label})
                    else: print(f"    [Cảnh báo Fig 13] Bỏ qua file, không thể phân tích kích thước: {f}")

        elif experiment_type == "txdelay":
            file_regex = re.compile(r'.*-txdelay-(\d+)\.txt$') # Bỏ 'ms'
            for f in os.listdir(dir_path):
                match = file_regex.match(f)
                if match:
                    delay_ms = int(match.group(1))
                    tps, _ = parse_log_file(os.path.join(dir_path, f))
                    if tps: data_fig14_txdelay.append({"system": system_name, "delay_ms": delay_ms, "throughput": tps})

        elif experiment_type == "networking":
            file_regex = re.compile(r'.*?(\d+|NoLimit)-(\d+)ms\.txt$')
            for f in os.listdir(dir_path):
                if os.path.isdir(os.path.join(dir_path, f)): continue
                match = file_regex.match(f)
                if match:
                    bandwidth_str, rtt_ms = match.group(1), int(match.group(2))
                    if bandwidth_str == "NoLimit":
                        bandwidth_label, bandwidth_sort_key = "No Limit", np.inf
                    else:
                        bandwidth_mbps = int(bandwidth_str)
                        bandwidth_sort_key = bandwidth_mbps
                        if bandwidth_mbps == 100: bandwidth_label = "100 Mbps"
                        elif bandwidth_mbps == 1000: bandwidth_label = "1 Gbps"
                        elif bandwidth_mbps == 10000: bandwidth_label = "10 Gbps"
                        else: bandwidth_label = f"{bandwidth_mbps} Mbps"
                    tps, lat = parse_log_file(os.path.join(dir_path, f))
                    if tps:
                        data_fig15_16_net.append({
                            "System": system_name, "Bandwidth": bandwidth_label,
                            "bandwidth_sort_key": bandwidth_sort_key, "RTT (ms)": rtt_ms,
                            "Throughput (TPS)": tps, "Latency (ms)": lat
                        })
    
    print("\n...Quét tất cả log hoàn tất!")
    
    return {
        "fig5": pd.DataFrame(data_fig5_clients), "fig7": pd.DataFrame(data_fig7_nodes),
        "fig8": pd.DataFrame(data_fig8_kafka_ops), "fig9": pd.DataFrame(data_fig9_dist),
        "fig10": pd.DataFrame(data_fig10_workload), "fig11": pd.DataFrame(data_fig11_db),
        "fig12": pd.DataFrame(data_fig12_blksize), "fig13": pd.DataFrame(data_fig13_txsize),
        "fig14": pd.DataFrame(data_fig14_txdelay), "fig15_16": pd.DataFrame(data_fig15_16_net)
    }

# --- PHẦN 3: CÁC HÀM VẼ BIỂU ĐỒ ---

# *** THAY ĐỔI MỚI: Định dạng Y-axis (trục tung) để hiển thị số đầy đủ, không dùng ký hiệu khoa học ***
# Sẽ được áp dụng cho TẤT CẢ các biểu đồ có thang logarit
y_axis_formatter = ticker.FuncFormatter(lambda y, _: f'{y:.0f}')

def plot_figure_5(df):
    if df.empty:
        print("\n[Bỏ qua Figure 5]: Không tìm thấy dữ liệu 'clients'.")
        return
    print("\nĐang vẽ Figure 5 (Clients)...")
    df = df.sort_values(by="clients")
    client_ticks = sorted(df['clients'].unique())
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle('Figure 5: Ảnh hưởng của Số lượng Client', fontsize=16)
    sns.lineplot(data=df, x="clients", y="throughput", hue="system", style="system", markers=True, dashes=True, ax=ax1, linewidth=2.5)
    ax1.set_title('Thông lượng (TPS)', fontsize=14)
    ax1.set_xlabel('Số lượng Client (Concurrency)', fontsize=12)
    ax1.set_ylabel('Thông lượng (TPS)', fontsize=12)
    ax1.set_yscale('log')
    ax1.legend(title='Hệ thống')
    ax1.set_xticks(client_ticks)
    ax1.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax1.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
    sns.lineplot(data=df, x="clients", y="latency", hue="system", style="system", markers=True, dashes=True, ax=ax2, linewidth=2.5)
    ax2.set_title('Độ trễ (Latency)', fontsize=14)
    ax2.set_xlabel('Số lượng Client (Concurrency)', fontsize=12)
    ax2.set_ylabel('Độ trễ Trung bình (ms)', fontsize=12)
    ax2.legend(title='Hệ thống')
    ax2.set_xticks(client_ticks)
    ax2.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax2.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("figure_5_clients_benchmark.png")
    print("✅ Đã lưu: figure_5_clients_benchmark.png")
    plt.close()

def plot_figure_7(df):
    if df.empty:
        print("\n[Bỏ qua Figure 7]: Không tìm thấy dữ liệu 'nodes'.")
        return
    print("\nĐang vẽ Figure 7 (Nodes)...")
    df = df.sort_values(by="nodes")
    node_ticks = sorted(df['nodes'].unique())
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle('Figure 7: Ảnh hưởng của Số lượng Node', fontsize=16)
    sns.lineplot(data=df, x="nodes", y="throughput", hue="system", style="system", markers=True, dashes=True, ax=ax1, linewidth=2.5)
    ax1.set_title('Thông lượng (TPS)', fontsize=14)
    ax1.set_xlabel('Số lượng Node (Server)', fontsize=12)
    ax1.set_ylabel('Thông lượng (TPS)', fontsize=12)
    ax1.set_yscale('log')
    ax1.legend(title='Hệ thống')
    ax1.set_xticks(node_ticks)
    ax1.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax1.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
    sns.lineplot(data=df, x="nodes", y="latency", hue="system", style="system", markers=True, dashes=True, ax=ax2, linewidth=2.5)
    ax2.set_title('Độ trễ (Latency)', fontsize=14)
    ax2.set_xlabel('Số lượng Node (Server)', fontsize=12)
    ax2.set_ylabel('Độ trễ Trung bình (ms)', fontsize=12)
    ax2.legend(title='Hệ thống')
    ax2.set_xticks(node_ticks)
    ax2.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax2.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("figure_7_nodes_benchmark.png")
    print("✅ Đã lưu: figure_7_nodes_benchmark.png")
    plt.close()

def plot_figure_8(df):
    if df.empty:
        print("\n[Bỏ qua Figure 8]: Không tìm thấy dữ liệu 'kafka-counters'.")
        return
    print("\nĐang vẽ Figure 8 (Kafka Ops)...")
    df = df.sort_values(by="Nodes")
    node_ticks = sorted(df['Nodes'].unique())
    plt.figure(figsize=(10, 7))
    ax = sns.lineplot(data=df, x="Nodes", y="Operations", hue="Operation Type", style="Operation Type", markers=True, dashes=True, linewidth=2.5)
    ax.set_title('Figure 8: Ảnh hưởng của Số Node (Thao tác Kafka)', fontsize=16)
    ax.set_xlabel('Số lượng Node (Server)', fontsize=12)
    ax.set_ylabel('Số lượng Thao tác Kafka', fontsize=12)
    ax.set_yscale('log')
    ax.legend(title='Loại Thao tác')
    ax.set_xticks(node_ticks)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
    plt.tight_layout()
    plt.savefig("figure_8_kafka_ops_benchmark.png")
    print("✅ Đã lưu: figure_8_kafka_ops_benchmark.png")
    plt.close()

def plot_figure_9_10_11(df9, df10, df11):
    if df9.empty:
        print("\n[Bỏ qua Figure 9]: Không tìm thấy dữ liệu 'distribution'.")
    else:
        print("\nĐang vẽ Figure 9 (Distribution)...")
        df9['Distribution'] = pd.Categorical(df9['Distribution'], categories=["Uniform", "Latest", "Zipfian"], ordered=True)
        df9 = df9.sort_values("Distribution")
        plt.figure(figsize=(10, 7))
        ax = sns.barplot(data=df9, x="Distribution", y="Throughput (TPS)", hue="System", palette="muted")
        ax.set_title('Figure 9: Ảnh hưởng của Phân phối Khóa (Workload A)', fontsize=16)
        ax.set_xlabel('Loại Phân phối (Access Distribution)', fontsize=12)
        ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
        ax.set_yscale('log')
        ax.legend(title='Hệ thống')
        ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
        plt.tight_layout()
        plt.savefig("figure_9_distribution.png")
        print("✅ Đã lưu: figure_9_distribution.png")
        plt.close()

    if df10.empty:
        print("\n[Bỏ qua Figure 10]: Không tìm thấy dữ liệu 'workload'.")
    else:
        print("\nĐang vẽ Figure 10 (Workload)...")
        df10['Workload'] = pd.Categorical(df10['Workload'], categories=["Workload A", "Workload B", "Workload C"], ordered=True)
        df10 = df10.sort_values("Workload")
        plt.figure(figsize=(10, 7))
        ax = sns.barplot(data=df10, x="Workload", y="Throughput (TPS)", hue="System", palette="muted")
        ax.set_title('Figure 10: Hiệu suất của các YCSB Workload', fontsize=16)
        ax.set_xlabel('YCSB Workloads', fontsize=12)
        ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
        ax.set_yscale('log')
        ax.legend(title='Hệ thống')
        ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
        plt.tight_layout()
        plt.savefig("figure_10_workload.png")
        print("✅ Đã lưu: figure_10_workload.png")
        plt.close()

    if df11.empty:
        print("\n[Bỏ qua Figure 11]: Không tìm thấy dữ liệu 'database'.")
    else:
        print("\nĐang vẽ Figure 11 (Database)...")
        df11['Workload'] = pd.Categorical(df11['Workload'], categories=["Workload A", "Workload B", "Workload C"], ordered=True)
        df11 = df11.sort_values("Workload")
        plt.figure(figsize=(10, 7))
        ax = sns.barplot(data=df11, x="Workload", y="Throughput (TPS)", hue="Database", palette="muted")
        ax.set_title('Figure 11: Ảnh hưởng của Cơ sở dữ liệu (Veritas-Kafka)', fontsize=16)
        ax.set_xlabel('YCSB Workloads', fontsize=12)
        ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
        ax.legend(title='Cơ sở dữ liệu')
        ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
        plt.tight_layout()
        plt.savefig("figure_11_database.png")
        print("✅ Đã lưu: figure_11_database.png")
        plt.close()

def plot_figure_12(df):
    if df.empty:
        print("\n[Bỏ qua Figure 12]: Không tìm thấy dữ liệu 'blksize'.")
        return
    print("\nĐang vẽ Figure 12 (Block Size)...")
    df = df.sort_values(by="blksize")
    plt.figure(figsize=(10, 7))
    ax = sns.lineplot(data=df, x="blksize", y="throughput", hue="system", style="system", markers=True, dashes=True, linewidth=2.5)
    ax.set_title('Figure 12: Ảnh hưởng của Kích thước Khối (Block Size)', fontsize=16)
    ax.set_xlabel('Kích thước Khối (Số lượng giao dịch)', fontsize=12)
    ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
    ax.set_xscale('log') 
    ax.set_yscale('log') 
    ax.legend(title='Hệ thống')
    ax.get_xaxis().set_major_formatter(ticker.FuncFormatter(lambda x, _: '{:g}'.format(x)))
    ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
    plt.tight_layout()
    plt.savefig("figure_12_block_size.png")
    print("✅ Đã lưu: figure_12_block_size.png")
    plt.close()
    
def plot_figure_13_14(df13, df14):
    if df13.empty:
        print("\n[Bỏ qua Figure 13]: Không tìm thấy dữ liệu 'txsizes'.")
    else:
        print("\nĐang vẽ Figure 13 (Record Size)...")
        df13 = df13.sort_values(by="size_kb") 
        df13['size_label'] = df13['size_label'].astype(str)
        df13 = df13.sort_values(by='size_kb')
        plt.figure(figsize=(10, 7))
        ax = sns.lineplot(data=df13, x="size_label", y="throughput", hue="system", style="system", markers=True, dashes=True, linewidth=2.5)
        ax.set_title('Figure 13: Ảnh hưởng của Kích thước Giao dịch', fontsize=16)
        ax.set_xlabel('Kích thước Key-Value', fontsize=12) 
        ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
        ax.set_yscale('log')
        ax.legend(title='Hệ thống')
        ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
        plt.tight_layout()
        plt.savefig("figure_13_record_size.png")
        print("✅ Đã lưu: figure_13_record_size.png")
        plt.close()

    if df14.empty:
        print("\n[Bỏ qua Figure 14]: Không tìm thấy dữ liệu 'txdelay'.")
    else:
        print("\nĐang vẽ Figure 14 (Processing Time)...")
        df14 = df14.sort_values(by="delay_ms")
        df14['delay_ms_str'] = df14['delay_ms'].astype(str) + ' ms'
        plt.figure(figsize=(10, 7))
        ax = sns.lineplot(data=df14, x="delay_ms_str", y="throughput", hue="system", style="system", markers=True, dashes=True, linewidth=2.5)
        ax.set_title('Figure 14: Ảnh hưởng của Thời gian Xử lý Giao dịch', fontsize=16)
        ax.set_xlabel('Thời gian Xử lý (ms)', fontsize=12)
        ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
        ax.set_yscale('log')
        ax.legend(title='Hệ thống')
        ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
        plt.tight_layout()
        plt.savefig("figure_14_processing_time.png")
        print("✅ Đã lưu: figure_14_processing_time.png")
        plt.close()

def plot_figure_15_16(df):
    """
    Vẽ Figure 15 & 16: Networking.
    *** ĐÃ SỬA LỖI: Sửa 'AttributeError' và thêm 'padding' cho trục Y ***
    """
    if df.empty:
        print("\n[Bỏ qua Figure 15 & 16]: Không tìm thấy dữ liệu 'networking'.")
        return

    print("\nĐang vẽ Figure 15 & 16 (Networking)...")
    df = df.sort_values(by=["bandwidth_sort_key", "RTT (ms)"])
    rtt_ticks = sorted(df['RTT (ms)'].unique())

    # --- Biểu đồ 1: Thông lượng (Figure 15) ---
    g_tps = sns.relplot(
        data=df, x="RTT (ms)", y="Throughput (TPS)",
        hue="Bandwidth", style="Bandwidth", col="System",
        kind="line", markers=True, dashes=True, linewidth=2.5,
        height=6, aspect=1,
        facet_kws={'sharey': False}  # *** ĐÃ SỬA LỖI: Đặt sharey=False vào đúng chỗ ***
    )
    g_tps.fig.suptitle('Figure 15: Ảnh hưởng của Mạng (Thông lượng)', y=1.03, fontsize=16)
    g_tps.set_axis_labels("Thời gian Trễ Mạng (RTT) [ms]", "Thông lượng (TPS)")
    g_tps.set_titles("{col_name}")
    g_tps.set(yscale='log')
    
    for ax in g_tps.axes.flat:
        ax.get_yaxis().set_major_formatter(y_axis_formatter) # <-- ĐÃ ÁP DỤNG
        ax.set_xticks(rtt_ticks)
        ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
        
        # Thêm "khoảng thở" 20% (padding) cho trục Y (thang log)
        if ax.get_ylim()[0] > 0 and ax.get_ylim()[1] > 0:
            ymin, ymax = ax.get_ylim()
            ax.set_ylim(ymin * 0.8, ymax * 1.2) # Thêm 20% padding
    
    g_tps.savefig("figure_15_networking_throughput.png")
    print("✅ Đã lưu: figure_15_networking_throughput.png")
    plt.close()

    # --- Biểu đồ 2: Độ trễ (Figure 16) ---
    g_lat = sns.relplot(
        data=df, x="RTT (ms)", y="Latency (ms)",
        hue="Bandwidth", style="Bandwidth", col="System",
        kind="line", markers=True, dashes=True, linewidth=2.5,
        height=6, aspect=1,
        facet_kws={'sharey': False}  # *** ĐÃ SỬA LỖI: Đặt sharey=False vào đúng chỗ ***
    )
    g_lat.fig.suptitle('Figure 16: Ảnh hưởng của Mạng (Độ trễ)', y=1.03, fontsize=16)
    g_lat.set_axis_labels("Thời gian Trễ Mạng (RTT) [ms]", "Độ trễ Trung bình (ms)")
    g_lat.set_titles("{col_name}")

    for ax in g_lat.axes.flat:
        ax.set_xticks(rtt_ticks)
        ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
        ax.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
        
        # Thêm "khoảng thở" 10% (padding) cho trục Y (thang linear)
        if ax.get_ylim()[1] > 0:
            ymin, ymax = ax.get_ylim()
            padding = (ymax - ymin) * 0.1
            ax.set_ylim(max(0, ymin - padding), ymax + padding)

    g_lat.savefig("figure_16_networking_latency.png")
    print("✅ Đã lưu: figure_16_networking_latency.png")
    plt.close()

# --- PHẦN 4: HÀM CHẠY CHÍNH ---

def main():
    # Cài đặt thư viện
    try:
        import pandas, matplotlib, seaborn, numpy
    except ImportError:
        print("Đang cài đặt các thư viện bắt buộc: pandas, matplotlib, seaborn, numpy...")
        try:
            import subprocess
            subprocess.check_call(["pip", "install", "pandas", "matplotlib", "seaborn", "numpy"])
            print("Cài đặt hoàn tất. Vui lòng chạy lại script.")
        except Exception as e:
            print(f"Lỗi khi cài đặt qua pip: {e}")
            print("Vui lòng tự cài đặt thư viện bằng lệnh: pip install pandas matplotlib seaborn numpy")
        return

    # 1. Thu thập tất cả dữ liệu
    all_data = collect_all_data()
    
    # 2. Tạo các DataFrame
    df_fig5 = all_data.get("fig5", pd.DataFrame())
    df_fig7 = all_data.get("fig7", pd.DataFrame())
    df_fig8 = all_data.get("fig8", pd.DataFrame())
    df_fig9 = all_data.get("fig9", pd.DataFrame())
    df_fig10 = all_data.get("fig10", pd.DataFrame())
    df_fig11 = all_data.get("fig11", pd.DataFrame())
    df_fig12 = all_data.get("fig12", pd.DataFrame())
    df_fig13 = all_data.get("fig13", pd.DataFrame())
    df_fig14 = all_data.get("fig14", pd.DataFrame())
    df_fig15_16 = all_data.get("fig15_16", pd.DataFrame())

    # 3. Vẽ tất cả biểu đồ
    plot_figure_5(df_fig5)
    plot_figure_7(df_fig7)
    plot_figure_8(df_fig8)
    plot_figure_9_10_11(df_fig9, df_fig10, df_fig11)
    plot_figure_12(df_fig12) 
    plot_figure_13_14(df_fig13, df_fig14)
    plot_figure_15_16(df_fig15_16)
    
    print("\n=============================================")
    print("      Hoàn tất! Tất cả biểu đồ đã được tạo.")
    print("=============================================")

if __name__ == "__main__":
    main()