# Business Dashboard App

This dashboard provides a comprehensive view of key financial metrics and client data, enabling businesses in the investment industry to make informed decisions. 

Features include:
- **Client Overview:** Display detailed client information including investment history, risk profile, and personal details.
- **Financial Metrics:** Track key financial metrics such as Asset Under Management (AUM), Revenue, Annual Contibutions
- **Investment Analysis:** Analyze investment trends and patterns with interactive charts and graphs.

By providing a centralized platform for all essential business data, this dashboard enhances efficiency, promotes data-driven decision making, and ultimately drives business growth in the investment industry.


## Infrastructure

1. CSV data is loaded into Postgres with python script `load_postgres_db.py` 
2. Postgres database is migrated to Google BigQuery with Airbyte via Terraform
3. Streamlit app is pulling data from Google BigQuery

> Terraform

[!NOTE]
Terraform is an open-source tool for managing cloud infrastructure with code, commonly known as Infrastructure as Code (IaC). With its declarative syntax, Terraform enables you to define and provision resources across various cloud platforms, ensuring consistency and ease of management since resources are declared as code and not deployed by hand.

    
> Airbyte

[!NOTE]
Airbyte is an open-source data movement infrastructure for building extract and load (EL) data pipelines. 

[!TIP]
When running Airbyte locally, the API server runs on localhost:8000 so the url is 
http://localhost:8000



## Installation

Follow these steps to set up the Business Dashboard App:

1. **Clone the repository**

    Open a terminal and run the following git command:

    ```bash
    git clone https://github.com/miroslaw-steblik/business-dashboard-gcp-app.git
    ```



2. **Install the dependencies**

    Navigate to the directory where you cloned the repository and install the required Python packages:

    ```
    cd business-dashboard-app
    poetry install
    ```

3. **Set up the database**

    Run the `load_postgres_db.py` script to load the CSV data into Postgres:

    ```bash
    poetry run python load_postgres_db.py
    ```

4. **Migrate to Google BigQuery**
    > Terraform

    - Add `infra/` directory in project working directory
    - Add `provider.tf`
    - Configure `main.tf`, `variable.tf` [variable template] and `vars.tfvars` [customised variables (add to `.gitignore`)]

    Run: 

        terraform init

    To load customised variables run: 

        terraform validate
        terraform plan --out temp -var-file=vars.tfvars
        
        
    To initialize run: 

        terraform apply "temp" 

    > Airbyte
    > Sync Postgres with BigQuery

    Run the `sync_airbyte_connecion.py` script to sync database


5. **Run the app**

    Start the Streamlit app:

    ```bash
    poetry run streamlit run app.py
    ```

    The app should now be running at `http://localhost:8501`.

Please note that this guide assumes you have Python and Poetry installed on your machine, and that you have a Postgres database set up. If not, you'll need to install Python and Poetry, and set up a Postgres database before you can run the app.