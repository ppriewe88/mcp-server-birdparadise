from typing import Any

from pydantic import BaseModel

from mcp_server.database.utils.connect import establish_database_connection

Row = dict[str, Any]
Status = dict[str, str]
QueryResult = list[Row] | Status | None


class Structured(BaseModel):
    data: dict[str, Any]


class DatabaseCapabilities:
    def __init__(self) -> None:
        self.connection: Any = establish_database_connection()

    def _make_query(
        self,
        input_query: str,
        procedure: bool = False,
        params: Any = None,
    ) -> QueryResult:
        """Send query to the database and return results."""
        try:
            cursor = self.connection.cursor(as_dict=True)

            if procedure and params:
                cursor.execute(input_query, params)
            else:
                cursor.execute(input_query)

            if cursor.description:
                rows: list[Row] = cursor.fetchall()
                return rows

            try:
                rows = cursor.fetchall()
                if rows:
                    self.connection.commit()
                    return rows
            except Exception:
                self.connection.commit()
                result: Status = {
                    "status": "success (fallback)",
                    "message": "Query executed, no data returned.",
                }
                return result

            self.connection.commit()
            result = {
                "status": "success",
                "message": "Query executed, no data returned.",
            }
            return result

        except Exception as e:
            print(f"Error during execution of sql-query:\n{e}")
            return None

    def _to_structured(self, query_result: QueryResult) -> Structured:
        """Convert a QueryResult to a Structured response, keyed by row id."""
        if isinstance(query_result, list):
            data: dict[str, Any] = {
                str(i + 1): row for i, row in enumerate(query_result)
            }
        elif isinstance(query_result, dict):
            data = query_result
        else:
            data = {"error": "Query returned no result."}
        return Structured(data=data)

    def find_customer(self, customer_id: int) -> Structured:
        """Get customer entry by id."""
        query = f"SELECT * FROM customers WHERE id = {customer_id}"
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def show_customers(self) -> Structured:
        """Get a list of customers with their IDs."""
        query = "SELECT id, name FROM customers"
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    def show_unpaid_invoices(self) -> Structured:
        """Get a list of unpaid and overdue invoices."""
        query = """
            SELECT i.id, i.order_id, c.name AS customer_name,
                i.total_price,
                FORMAT(i.due_date, 'yyyy-MM-dd') AS due_date, invs.name AS status
            FROM invoices i
            JOIN orders o ON o.id = i.order_id
            JOIN customers c ON c.id = o.customer_id
            JOIN invoices_status invs ON invs.id = i.status_id
            WHERE invs.name LIKE '%unpaid%' OR invs.name LIKE '%overdue%'
            ORDER BY i.due_date DESC"""
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    def show_products(self) -> Structured:
        """Get a list of products with prices and inventory."""
        query = """
            SELECT p.id, p.name, p.sale_price, p.description,
                i.stock, invsl.name AS storage
            FROM products p
            JOIN inventory i ON i.product_id = p.id
            JOIN inventory_storagelocations invsl ON invsl.id = i.storage_location_id"""
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    def show_unpaid_invoices_for_customer(self, customer_id: int) -> Structured:
        """Get unpaid/overdue invoices for a specific customer."""
        query = f"""
            SELECT i.id, i.order_id, i.total_price, i.total_price_discounted,
                FORMAT(i.due_date, 'yyyy-MM-dd') AS due_date, invs.name AS status
            FROM invoices i
            JOIN orders o ON o.id = i.order_id
            JOIN invoices_status invs ON invs.id = i.status_id
            WHERE o.customer_id = {customer_id}
              AND (invs.name LIKE '%unpaid%' OR invs.name LIKE '%overdue%')
            ORDER BY i.due_date DESC"""
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    def pay_invoice(self, invoice_id: int) -> str:
        """Pay an invoice and check for discounts via stored procedure."""
        query = "EXEC spChangeInvoiceStatusAndCheckDiscount @invoiceID = %s, @newStatusID = 2"
        query_result = self._make_query(query, procedure=True, params=invoice_id)
        if query_result is None:
            return f"Fehler: Rechnung {invoice_id} konnte nicht bezahlt werden."
        return f"Rechnung {invoice_id} wurde erfolgreich bezahlt."