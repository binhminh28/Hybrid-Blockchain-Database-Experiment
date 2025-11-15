import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
# Import thêm thư viện để định dạng trục
from matplotlib.ticker import ScalarFormatter

# 1. ĐỊNH NGHĨA CÁC HỆ THỐNG VÀ THƯ MỤC LOG
SYSTEM_CONFIGS = {
    "Veritas (Kafka)": {
        "log_dir": "veritas_kafka_net_logs",
        "file_prefix": "veritas-", # Ví dụ: veritas-100-5ms.txt
    },
    "Veritas (TM)": {
        "log_dir": "veritas_tendermint_net_logs",
        "file_prefix": "veritas-net-",
    },
    "BigchainDB": {
        "log_dir": "bigchaindb_logs",
        "file_prefix": "bigchaindb-", # Tên giả định
    },
    # Thêm các hệ thống khác nếu cần
}

def parse_log_file(filepath):
    """
    Đọc một file log duy nhất và trích xuất thông lượng (throughput) và độ trễ (latency).
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            
            throughput_match = re.search(r'Throughput.*: (\d+) req/s', content)
            latency_match = re.search(r'Average latency: ([\d.]+) ms', content)
            
            if throughput_match and latency_match:
                throughput = int(throughput_match.group(1))
                latency = float(latency_match.group(1))
                return throughput, latency
                
    except Exception as e:
        print(f"    Lỗi khi đọc file {filepath}: {e}")
        
    return None, None

def collect_data():
    """
    Quét tất cả các thư mục log đã định nghĩa và thu thập dữ liệu.
    Xử lý chính xác tên file "NoLimit".
    """
    all_data = []
    print("Bắt đầu quét log (Thí nghiệm Mạng)...")

    for system_name, config in SYSTEM_CONFIGS.items():
        log_dir = config["log_dir"]
        prefix = config["file_prefix"]
        
        if not os.path.isdir(log_dir):
            print(f"- Bỏ qua '{system_name}': Không tìm thấy thư mục '{log_dir}'")
            continue
            
        print(f"+ Đang xử lý '{system_name}' trong '{log_dir}'...")
        
        # Regex MỚI: trích xuất (số HOẶC "NoLimit") và RTT
        file_regex = re.compile(f'^{re.escape(prefix)}(\d+|NoLimit)-(\d+)ms\.txt$')

        for filename in os.listdir(log_dir):
            filepath = os.path.join(log_dir, filename)
            
            if os.path.isdir(filepath):
                continue
            
            match = file_regex.match(filename)
            
            if match:
                bandwidth_str = match.group(1) # Đây là '100' hoặc 'NoLimit'
                rtt_ms = int(match.group(2))
                
                # Xử lý chuỗi bandwidth_str
                if bandwidth_str == "NoLimit":
                    bandwidth_label = "No Limit"
                    bandwidth_sort_key = np.inf # Dùng vô cực để "No Limit" luôn ở cuối
                else:
                    bandwidth_mbps = int(bandwidth_str)
                    bandwidth_sort_key = bandwidth_mbps
                    if bandwidth_mbps == 100:
                        bandwidth_label = "100 Mbps"
                    elif bandwidth_mbps == 1000:
                        bandwidth_label = "1 Gbps"
                    elif bandwidth_mbps == 10000:
                        bandwidth_label = "10 Gbps"
                    else:
                        bandwidth_label = f"{bandwidth_mbps} Mbps"

                # Đọc file benchmark
                throughput, latency = parse_log_file(filepath)
                
                if throughput is not None:
                    all_data.append({
                        "System": system_name,
                        "Bandwidth": bandwidth_label,
                        "bandwidth_sort_key": bandwidth_sort_key, # Dùng để sắp xếp
                        "RTT (ms)": rtt_ms,
                        "Throughput (TPS)": throughput,
                        "Latency (ms)": latency
                    })
            else:
                print(f"  ! Cảnh báo: Bỏ qua file có tên không khớp '{filename}'")

    print("...Quét log hoàn tất!")
    return all_data

def plot_charts(data):
    """
    Sử dụng dữ liệu đã thu thập để vẽ và lưu 2 biểu đồ riêng biệt.
    *** ĐÃ CẬP NHẬT ***: Bỏ thang log (Y-axis) và định dạng lại trục (X-axis).
    """
    if not data:
        print("Không tìm thấy dữ liệu benchmark hợp lệ. Đã dừng vẽ biểu đồ.")
        return

    df = pd.DataFrame(data)
    
    # Sắp xếp theo RTT (trục X) và Băng thông (để thứ tự legend đẹp)
    df = df.sort_values(by=["bandwidth_sort_key", "RTT (ms)"])

    print("\nDataFrame dữ liệu đã trích xuất:")
    print(df)

    # Thiết lập giao diện biểu đồ
    sns.set_theme(style="whitegrid")
    
    # Lấy số lượng hệ thống tìm thấy để điều chỉnh chiều cao
    num_systems = len(df['System'].unique())
    if num_systems == 0:
        print("Không có dữ liệu hợp lệ để vẽ.")
        return
        
    # Lấy danh sách các giá trị RTT (trục hoành) để hiển thị chính xác
    rtt_ticks = sorted(df['RTT (ms)'].unique())

    # --- Biểu đồ 1: Thông lượng (Figure 15) ---
    print("Đang vẽ Figure 15 (Thông lượng)...")
    
    g_tps = sns.relplot(
        data=df,
        x="RTT (ms)",
        y="Throughput (TPS)",
        hue="Bandwidth",
        style="Bandwidth",
        col="System",
        kind="line",
        markers=True,
        dashes=True,
        linewidth=2.5,
        height=6,
        aspect=1
    )
    
    g_tps.fig.suptitle('Figure 15: Ảnh hưởng của Mạng (Thông lượng)', y=1.03, fontsize=16)
    g_tps.set_axis_labels("Thời gian Trễ Mạng (RTT) [ms]", "Thông lượng (TPS)")
    g_tps.set_titles("{col_name}")
    
    for ax in g_tps.axes.flat:
        ax.get_yaxis().set_major_formatter(ScalarFormatter())
        ax.get_yaxis().set_minor_formatter(ScalarFormatter())
        ax.set_xticks(rtt_ticks) # Ghi chính xác các số trên trục hoành
        ax.set_xticklabels(rtt_ticks)

    
    output_filename_tps = "figure_15_networking_throughput.png"
    g_tps.savefig(output_filename_tps)
    print(f"✅ Đã lưu: {output_filename_tps}")
    plt.close()

    # --- Biểu đồ 2: Độ trễ (Figure 16) ---
    print("Đang vẽ Figure 16 (Độ trễ)...")

    g_lat = sns.relplot(
        data=df,
        x="RTT (ms)",
        y="Latency (ms)",
        hue="Bandwidth",
        style="Bandwidth",
        col="System",
        kind="line",
        markers=True,
        dashes=True,
        linewidth=2.5,
        height=6,
        aspect=1
    )
    
    g_lat.fig.suptitle('Figure 16: Ảnh hưởng của Mạng (Độ trễ)', y=1.03, fontsize=16)
    g_lat.set_axis_labels("Thời gian Trễ Mạng (RTT) [ms]", "Độ trễ Trung bình (ms)")
    g_lat.set_titles("{col_name}")

    # *** THAY ĐỔI THEO YÊU CẦU ***
    # 1. Định dạng trục X (trục hoành) để hiển thị chính xác các mốc
    for ax in g_lat.axes.flat:
        ax.set_xticks(rtt_ticks)
        ax.set_xticklabels(rtt_ticks)

    output_filename_lat = "figure_16_networking_latency.png"
    g_lat.savefig(output_filename_lat)
    print(f"✅ Đã lưu: {output_filename_lat}")
    plt.close()

# --- Hàm chính để chạy ---
if __name__ == "__main__":
    benchmark_data = collect_data()
    plot_charts(benchmark_data)