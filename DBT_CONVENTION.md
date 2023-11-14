Welcome to your new dbt project!


### Using the starter project

Try running the following commands:
- dbt run
- dbt test

### Some DBT PROJECT CONVENTION
- profiles.yml:
    - Nên lưu bên ngoài thư mục repo 
    - Hoặc nếu để profile trong repo thì thông tin kết nối được export từ biến môi trường dùng biến : $Env:DBT_PROFILES_DIR = '...', add vào trong file môi trường của .dbtenv/Scripts/

- Thư mục legacy : lưu thông tin code cũ đã/ đang migration để đối chiếu 

- Data mart STRUCTURE: 
    - marts/<local_data_mart>/      : for model 
    - marts/<local_data_mart>_out/  : for view 
    - marts/<intermediate>/         : for temporary model (STM, STT, STC, STP)
        
- MODEL 
    - Do 1 số thuộc tính chuyên biệt mà dbt chưa hỗ trợ (index, partition), nên table cần được tạo sẵn trên database 
        - DDL script của table được đặt trong thư mục 03_#incremental (hoặc thự mục khác tương đương) và được chạy bởi CI/CD

    - Tên Model phải là duy nhất trên toàn project 

    - Model schema sẽ có cùng tên với model vd : dct_product.yml

    - Các bảng STM, STC , STT       : có phần suffix là động từ bổ trợ cho bảng chính 
        vd : DCT_CLIENT__MAP , DCT_CLIENT__DIFF, DCT_CLIENT__TEMP , DCT_CLIENT__PERMANENT_TEMP (STP) 
    - Các bảng temp hỗ trợ mapping, diff, temporary sẽ nằm trong thư mục intermediate với tên model được đặt theo quy tắc ở trên 

    - Schema cho các bảng STM, STC, STT nằm chung với bảng chính 

    - Model cần phải có tối thiểu 2 tag: 
        - chu kỳ chạy : monthly, daily 
        - tên của model phụ thuộc chính : dct_client, dct_contract 
        - ex : tags : [weekly, dct_contract]

- Document 
    - Được tổ chức theo CBL, trong thư mục doc , lưu markdown và data_flow diagram
    - document được tham chiếu = doc block từ trong model_schema.yml

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
