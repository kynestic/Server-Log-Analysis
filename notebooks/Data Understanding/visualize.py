import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class Visualize:
    def __init__(self, log_path):
        self.log_df = pd.read_csv(log_path)
        self.log_df['datetime'] = pd.to_datetime(self.log_df['datetime'], errors='coerce') 
        self.log_df['hour'] = self.log_df['datetime'].dt.hour
    
    def plot_errors_over_time(self):
        """
        Vẽ biểu đồ đường thể hiện số lượng lỗi và request hợp lệ qua thời gian.
        """
        self.log_df['request_type'] = self.log_df['status'].apply(lambda x: 'Lỗi' if x >= 400 else 'Hợp lệ')
        hourly_counts = self.log_df.groupby(['hour', 'request_type']).size().unstack(fill_value=0)
        
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=hourly_counts, markers=True, dashes=False)
        plt.title("Số lượng lỗi và Request hợp lệ qua thời gian")
        plt.xlabel("Giờ")
        plt.ylabel("Số Lượng")
        plt.xticks(range(24))
        plt.legend(title="Loại Request", labels=["Hợp lệ", "Lỗi"])
        plt.grid(True)
        plt.show()

    def plot_request_distribution(self):
        """
        Vẽ biểu đồ tròn thể hiện phân bố các loại request (GET, POST, PUT, DELETE,...).
        """
        request_counts = self.log_df['method'].value_counts()
        plt.figure(figsize=(8, 8))
        request_counts.plot.pie(autopct='%1.1f%%', startangle=90, colors=sns.color_palette("pastel"))
        plt.title("Phân bố các loại Request")
        plt.ylabel("")
        plt.show()

    def plot_bytes_sent_per_hour(self):
        """
        Vẽ biểu đồ đường thể hiện tổng lượng dữ liệu gửi đi mỗi giờ.
        """
        hourly_bytes_sent = self.log_df.groupby('hour')['bytes_sent'].sum()
        
        plt.figure(figsize=(10, 6))
        sns.lineplot(x=hourly_bytes_sent.index, y=hourly_bytes_sent.values, marker='o', color='blue')
        plt.title("Tổng lượng dữ liệu gửi đi mỗi giờ")
        plt.xlabel("Giờ")
        plt.ylabel("Tổng số byte gửi đi")
        plt.xticks(range(24))
        plt.grid(True)
        plt.show()


# if __name__ == "__main__":
#     path = r'D:\CODING\Project\Server Log Analysis\data\interim\parsed_logs.csv'

#     visualizer = Visualize(path)

#     visualizer.plot_errors_over_time()
#     visualizer.plot_request_distribution()
#     visualizer.plot_bytes_sent_per_hour()