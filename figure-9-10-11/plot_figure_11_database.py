import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# 1. ĐỊNH NGHĨA CÁC HỆ THỐNG VÀ THƯ MỤC LOG
# Script sẽ quét bên trong thư mục 'database_logs/'
# Fig 11 chỉ so sánh 2 hệ thống con của Veritas (Kafka)
SYSTEM_CONFIGS = {
    "Veritas (Kafka) - Redis": {
        "log_dir": "database_logs/veritas_kafka_redis_logs",
        "file_prefix": "veritas-kafka-workload", # Khớp với 'veritas-kafka-workloada.txt'
    },
    "Veritas (Kafka) - RediSQL": {
        "log_dir": "database_logs/veritas_kafka_redisql_logs",
        "file_prefix": "veritas-redisql-workload", # Khớp với 'veritas-redisql-workloada.txt'
    },
}

# Ánh xạ tên file sang tên hiển thị trên biểu đồ
WORKLOAD_MAP = {
    "a": "Workload A",
    "b": "Workload B",
    "c": "Workload C"
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
    print("Bắt đầu quét log (Thí nghiệm Database)...")

    for system_name, config in SYSTEM_CONFIGS.items():
        log_dir = config["log_dir"]
        prefix = config["file_prefix"]
        
        if not os.path.isdir(log_dir):
            print(f"- Bỏ qua '{system_name}': Không tìm thấy thư mục '{log_dir}'")
            continue
            
        print(f"+ Đang xử lý '{system_name}' trong '{log_dir}'...")
        
        # Regex để lấy loại workload (a, b, c) từ tên file
        file_regex = re.compile(f'^{re.escape(prefix)}(a|b|c)\.txt$')

        for filename in os.listdir(log_dir):
            match = file_regex.match(filename)
            if match:
                workload_key = match.group(1)
                workload_name = WORKLOAD_MAP.get(workload_key, workload_key)
                filepath = os.path.join(log_dir, filename)
                
                throughput = parse_log_file(filepath)
                
                if throughput is not None:
                    all_data.append({
                        "Database": system_name, # Đổi tên "System" thành "Database" cho legend
                        "Workload": workload_name,
                        "Throughput (TPS)": throughput,
                    })

    print("...Quét log hoàn tất!")
    return all_data

def plot_charts(data):
    """Vẽ biểu đồ cột (bar chart) cho Figure 11."""
    if not data:
        print("Không tìm thấy dữ liệu benchmark hợp lệ. Đã dừng vẽ biểu đồ.")
        return

    df = pd.DataFrame(data)
    # Đảm bảo thứ tự trục X (Workload) là đúng
    df['Workload'] = pd.Categorical(df['Workload'], categories=["Workload A", "Workload B", "Workload C"], ordered=True)
    df = df.sort_values("Workload")

    print("\nDataFrame dữ liệu đã trích xuất:")
    print(df)

    # Thiết lập giao diện biểu đồ
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 7))
    
    ax = sns.barplot(
        data=df, 
        x="Workload", 
        y="Throughput (TPS)", 
        hue="Database",
        palette="muted"
    )
    
    ax.set_title('Figure 11: Ảnh hưởng của Cơ sở dữ liệu (Veritas-Kafka)', fontsize=16)
    ax.set_xlabel('YCSB Workloads', fontsize=12)
    ax.set_ylabel('Thông lượng (TPS)', fontsize=12)
    
    # *** THAY ĐỔI THEO YÊU CẦU (KHÔNG DÙNG TRỤC LOG) ***
    # ax.set_yscale('log') # Figure 11 không dùng thang logarit
    ax.legend(title='Cơ sở dữ liệu')
    
    # Định dạng trục Y để hiển thị số chính xác (ví dụ: 40000)
    ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))

    # Tinh chỉnh layout và lưu file
    plt.tight_layout()
    output_filename = "figure_11_database.png"
    plt.savefig(output_filename)
    
    print(f"\n✅ Đã vẽ biểu đồ thành công và lưu tại: {output_filename}")

# --- Hàm chính để chạy ---
if __name__ == "__main__":
    benchmark_data = collect_data()
    plot_charts(benchmark_data)