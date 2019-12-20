# TwitterSentimentAnalysis_BigData20191
This is project of Bigdata course 20191


1.Download data sử dụng file data_downloader.py

 Sau khi download sẽ tự động sinh ra thư mục data/
 
2.Các bước xử lý data và training model đều được nằm trong file: scripts/final_report.ipynb

3.Sau khi có model (được lưu trong scripts/saved_model)

Ta có thể chạy demo sử dụng spark structed streaming bằng file demo_network.py

Lưu ý:
Trước khi chạy demo_network.py cần sử dụng netcat chạy một message server ở cổng 9999

nc -lk 9999

rồi thực hiện nhập text thông qua server. Bên phía client demo_network.py sẽ hiển thị kết quả dự đoán ngữ nghiax câu
 text ở output.
 
 + scripts/saved_model/model: model chưa được xử lý data -> kết quả khá thấp, acc=65%
 + scripts/saved_model/model4: model sử dụng Vowpal Wabbit Classifier của mllspark (một framework của Microsoft) kết
  quả cao nhất > 85%.
 + scripts/saved_model/model1_final: model của bài báo cáo có xử lý dữ liệu: > 76%