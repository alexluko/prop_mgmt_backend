from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import bigquery
from typing import Optional

app = FastAPI()

PROJECT_ID = "sp26-mgmt54500-alukowie"
DATASET = "property_mgmt"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


class IncomeCreate(BaseModel):
    amount: float
    date: str
    description: Optional[str] = None


class ExpenseCreate(BaseModel):
    amount: float
    date: str
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None


class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float


class PropertyUpdate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float


@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """
    try:
        results = bq.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )
    try:
        results = list(bq.query(query, job_config=job_config).result())
        if not results:
            raise HTTPException(status_code=404, detail="Property not found")
        return dict(results[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT income_id, property_id, amount, date, description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC, income_id DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )
    try:
        results = bq.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@app.post("/income/{property_id}")
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        check_query = f"""
            SELECT property_id
            FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """
        check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )
        existing = list(bq.query(check_query, job_config=check_config).result())
        if not existing:
            raise HTTPException(status_code=404, detail="Property not found")

        id_query = f"SELECT COALESCE(MAX(income_id), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET}.income`"
        next_id = list(bq.query(id_query).result())[0]["next_id"]

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.income`
            (income_id, property_id, amount, date, description)
            VALUES
            (@income_id, @property_id, @amount, @date, @description)
        """
        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("income_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", income.amount),
                bigquery.ScalarQueryParameter("date", "DATE", income.date),
                bigquery.ScalarQueryParameter("description", "STRING", income.description),
            ]
        )
        bq.query(insert_query, job_config=insert_config).result()

        return {
            "message": "Income record created",
            "income_id": next_id,
            "property_id": property_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")


@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT expense_id, property_id, amount, date, category, vendor, description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date DESC, expense_id DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )
    try:
        results = bq.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@app.post("/expenses/{property_id}")
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        check_query = f"""
            SELECT property_id
            FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """
        check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )
        existing = list(bq.query(check_query, job_config=check_config).result())
        if not existing:
            raise HTTPException(status_code=404, detail="Property not found")

        id_query = f"SELECT COALESCE(MAX(expense_id), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET}.expenses`"
        next_id = list(bq.query(id_query).result())[0]["next_id"]

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
            (expense_id, property_id, amount, date, category, vendor, description)
            VALUES
            (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
        """
        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("expense_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", expense.amount),
                bigquery.ScalarQueryParameter("date", "DATE", expense.date),
                bigquery.ScalarQueryParameter("category", "STRING", expense.category),
                bigquery.ScalarQueryParameter("vendor", "STRING", expense.vendor),
                bigquery.ScalarQueryParameter("description", "STRING", expense.description),
            ]
        )
        bq.query(insert_query, job_config=insert_config).result()

        return {
            "message": "Expense record created",
            "expense_id": next_id,
            "property_id": property_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")


@app.post("/properties")
def create_property(property_data: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        id_query = f"SELECT COALESCE(MAX(property_id), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET}.properties`"
        next_id = list(bq.query(id_query).result())[0]["next_id"]

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
            (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
            VALUES
            (@property_id, @name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
        """
        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )
        bq.query(insert_query, job_config=insert_config).result()

        return {"message": "Property created", "property_id": next_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")


@app.put("/properties/{property_id}")
def update_property(property_id: int, property_data: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        query = f"""
            UPDATE `{PROJECT_ID}.{DATASET}.properties`
            SET
                name = @name,
                address = @address,
                city = @city,
                state = @state,
                postal_code = @postal_code,
                property_type = @property_type,
                tenant_name = @tenant_name,
                monthly_rent = @monthly_rent
            WHERE property_id = @property_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )
        bq.query(query, job_config=job_config).result()
        return {"message": "Property updated", "property_id": property_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )
        bq.query(query, job_config=job_config).result()
        return {"message": "Property deleted", "property_id": property_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@app.get("/summary/{property_id}")
def property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        income_query = f"""
            SELECT COALESCE(SUM(amount), 0) AS total_income
            FROM `{PROJECT_ID}.{DATASET}.income`
            WHERE property_id = @property_id
        """
        expense_query = f"""
            SELECT COALESCE(SUM(amount), 0) AS total_expenses
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            WHERE property_id = @property_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        total_income = list(bq.query(income_query, job_config=job_config).result())[0]["total_income"]
        total_expenses = list(bq.query(expense_query, job_config=job_config).result())[0]["total_expenses"]

        return {
            "property_id": property_id,
            "total_income": total_income,
            "total_expenses": total_expenses,
            "net": total_income - total_expenses
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary query failed: {str(e)}")
