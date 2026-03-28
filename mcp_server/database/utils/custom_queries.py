get_storage_info = """
SELECT il.id, il.name, i.stock, i.min_stock 
FROM inventory i 
JOIN inventory_storagelocations il ON i.storage_location_id = il.id 
WHERE i.product_id = [product_id];"""

insert_order = """
INSERT INTO orders (customer_id, product_id, quantity, status_id)
VALUES ([customer_id], [product_id], [quantity], 1)"""

get_inserted_order = """
SELECT TOP 1 o.id AS order_id, o.customer_id, o.product_id, o.quantity, os.name AS status
FROM orders o
JOIN orders_status os ON o.status_id = os.id
WHERE o.customer_id = [customer_id]
ORDER BY o.created_at DESC;"""

get_corresponding_invoice = """
SELECT i.id, i.order_id, i.total_price, i.total_discount, i.total_price_discounted, i.due_limit, FORMAT(i.due_date, 'yyyy-MM-dd') AS due_date, i.overdue_fee, i.status_id, invs.name AS status
FROM invoices i
JOIN invoices_status invs ON invs.id = i.status_id
WHERE i.order_id = [order_id]"""

get_corresponding_pair_for_order = """
SELECT * 
FROM vw_0Aufträge
WHERE Bestell_ID = [order_id]"""

get_pair_for_customer = """
SELECT a.Bestellstatus, a.Auftragseingang, a.Kunden_ID, a.Produkt_ID, a.Bestellmenge, a.Rechnungs_ID, a.Umsatz, a.rabattierter_Umsatz, a.Mahngebühr, a.Zahlungsfrist, a.Zahltag, a.Rechnungsstatus, a.Status_Auftrag, p.name as produkt_name
FROM vw_0Aufträge a
JOIN products p ON a.Produkt_id = p.id
WHERE Kunden_ID = [customer_id] AND (Rechnungsstatus = 'id = 1 , unpaid' OR Rechnungsstatus = 'id = 3 , overdue')
"""

pay_invoice = """
EXEC spChangeInvoiceStatusAndCheckDiscount @invoiceID = ?, @newStatusID = 2"""

show_products = """
SELECT p.id AS Produkt_ID, p.name AS Produktname, p.sale_price AS Preis, p.description AS description, i.stock AS Bestand, i.min_stock as Mindestbestand, invsl.name as Lagername
FROM products p
JOIN inventory i ON i.product_id=p.id
JOIN inventory_storagelocations invsl ON invsl.id = i.storage_location_id """

show_revenues = """
SELECT *
FROM vw_4BasisFürUmsatzanalysen"""

show_customers = """
SELECT * 
FROM customers
"""