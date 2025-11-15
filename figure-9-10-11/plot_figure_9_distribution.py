import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# 1. ĐỊNH NGHĨA CÁC HỆ THỐNG VÀ THƯ MỤC LOG
# Script sẽ quét bên trong thư mục 'distribution_logs/'
SYSTEM_CONFIGS = {
    "Veritas (Kafka)": {
        "log_dir": "distribution_logs/veritas_kafka_logs",
        "file_prefix": "veritas-", # Khớp với 'veritas-uniform.txt'
    },
    "Veritas (TM)": {
        "log_dir": "distribution_logs/veritas_tendermint_logs",
        "file_prefix": "veritas-tm-", # Khớp với 'veritas-tm-uniform.txt'
    },
    "BigchainDB": {
        "log_dir": "distribution_logs/bigchaindb_logs",
        "file_prefix": "bigchaindb-", 
    },
    "BigchainDB (PV)": {
        "log_dir": "distribution_logs/bigchaindb_pv_logs",
        "file_prefix": "bigchaindb-pv-",
    },
    "BlockchainDB": {
        "log_dir": "distribution_logs/blockchaindb_logs",
        "file_prefix": "blockchaindb-",
    },
}

# Ánh xạ tên file sang tên hiển thị trên biểu đồ
DISTRIBUTION_MAP = {
    "uniform": "Uniform",
    "latest": "Latest",
    "zipfian": "Zipfian"
}

def parse_log_file(filepath):
    """Đọc file log và trích xuất thông lượng (TPS)."""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            throughput_match = re.search(r'Throughput.*: (\d+) req/s', content)
            if throughput_match:
                return int(throughput_match.group(1))
    except Exception as e:
        print(f"    Lỗi khi đọc file {filepath}: {e}")
    return None

def collect_data():
    """Quét các thư mục log và thu thập dữ liệu."""
    all_data = []
    print("Bắt đầu quét log (Thí nghiệm Distribution)...")

    for system_name, config in SYSTEM_CONFIGS.items():
        log_dir = config["log_dir"]
        prefix = config["file_prefix"]
        
        if not os.path.isdir(log_dir):
            print(f"- Bỏ qua '{system_name}': Không tìm thấy thư mục '{log_dir}'")
            continue
            
        print(f"+ Đang xử lý '{system_name}' trong '{log_dir}'...")
        
        # Regex để lấy loại phân phối từ tên file
        # (Ví dụ: 'veritas-uniform.txt' -> 'uniform')
        file_regex = re.compile(f'^{re.escape(prefix)}(uniform|latest|zipfian)\.txt$')

        for filename in os.listdir(log_dir):
            match = file_regex.match(filename)
            if match:
                dist_key = match.group(1)
                distribution_name = DISTRIBUTION_MAP.get(dist_key, dist_key)
                filepath = os.path.join(log_dir, filename)
                
                throughput = parse_log_file(filepath)
                
                if throughput is not None:
                    all_data.append({
                        "System": system_name,
                        "Distribution": distribution_name,
                        "Throughput (TPS)": throughput,
                    })

    print("...Quét log hoàn tất!")
    return all_data

def plot_charts(data):
    """Vẽ biểu đồ cột (bar chart) cho Figure 9."""
    if not data:
        print("Không tìm thấy dữ liệu benchmark hợp lệ. Đã dừng vẽ biểu đồ.")
        return

    df = pd.DataFrame(data)
    # Đảm bảo thứ tự trục X (Distribution) là đúng
    df['Distribution'] = pd.Categorical(df['Distribution'], categories=["Uniform", "Latest", "Zipfian"], ordered=True)
    df = df.sort_values("Distribution")

    print("\nDataFrame dữ liệu đã trích xuất:")
    print(df)

    # Thiết lập giao diện biểu đồ
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 7))
    
    ax = sns.barplot(
        data=df, 
        x="Distribution", 
        y="Throughput (TPS)", 
        hue="System",
        palette="muted"
    )
    
    ax.set_title('Figure 9: Ảnh hưởng của Phân phối Khóa (Workload A)', fontsize=16)
    ax.set_xlabel('Loại Phân phối (Access Distribution)', fontsize=12)
    ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
    ax.set_yscale('log') # Sử dụng trục Y logarit giống bài báo
    ax.legend(title='Hệ thống')
    
    # Định dạng trục Y để hiển thị số chính xác (ví dụ: 10000) thay vì (10^4)
    ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))

    # Tinh chỉnh layout và lưu file
    plt.tight_layout()
    output_filename = "figure_9_distribution.png"
    plt.savefig(output_filename)
    
    print(f"\n✅ Đã vẽ biểu đồ thành công và lưu tại: {output_filename}")

# --- Hàm chính để chạy ---
if __name__ == "__main__":
    benchmark_data = collect_data()
    plot_charts(benchmark_data)