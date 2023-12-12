Welcome to your new dbt project!


### Using the starter project

Try running the following commands :

- Parse project to check syntax :  dbt parse
- Testing models : dbt test
- Build models : dbt build --model <model_name>


### Some DBT PROJECT CONVENTION
- profiles.yml:
    - Should store outside repo directory 
    - If you want to store profiles.yml inside repo directory, then you have to use Environment Variable instead of plan text for credential infoamrtion 
        - If you using vscode, then environment variables are added into .vscode/setting.json 

- Data mart STRUCTURE: 
    - marts/<local_data_mart>/      : for model 
    - marts/<local_data_mart>_out/  : for view 
    - marts/<intermediate>/         : for temporary model (STM, STT, STC, STP)
    - marts/legacy/                 : for legacy / old source code , using for compare

- MODEL 
    - Do 1 số thuộc tính chuyên biệt mà dbt chưa hỗ trợ (index, partition), nên table cần được tạo sẵn trên database 
        - DDL script của table được đặt trong thư mục 03_#incremental (hoặc thự mục khác tương đương) và được chạy bởi CI/CD

    - Tên Model phải là duy nhất trên toàn project 

    - Model schema sẽ có cùng tên với model vd : dct_product.yml

    - Các bảng STM, STC , STT       : có phần suffix là động từ bổ trợ cho bảng chính , sau 2 dấu Underscore __ 
        vd : DCT_CLIENT__MAP , DCT_CLIENT__DIFF, DCT_CLIENT__TEMP , DCT_CLIENT__PERMANENT_TEMP (STP) 
    
    - Các bảng temp hỗ trợ mapping, diff, temporary sẽ nằm trong thư mục intermediate với tên model được đặt theo quy tắc ở trên 

    - Tuỳ trường hợp mà Temporary table có thể chọn materialize là : table (fix data), view, 

    - Schema cho các bảng STM, STC, STT nằm chung với bảng chính 

    - Model cần phải có tối thiểu 2 tag: 
        - chu kỳ chạy : monthly, daily 
        - tên của model phụ thuộc chính : dct_client, dct_contract 
        - ex : tags : [weekly, dct_contract]

- TEST 
    - Không nên lạm dụng test để tránh ảnh hưởng đến performance trong trường hợp đã đặt constraint vào target Table 
    - Model Map cần có test unique hoặc unique contraint để merge được vào model chính 

- Document 
    - Được tổ chức theo CBL, trong thư mục doc , lưu markdown và data_flow diagram
    - document được tham chiếu = doc block từ trong model_schema.yml

- Comment in Jinja :
    Following format :
        {#  /*handle table does not exists with do-nothing*/ #}
        
        {#  --handle table does not exists with do-nothing
        #}
        
        -- this line still display when dbt compile. While above line will not 

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
