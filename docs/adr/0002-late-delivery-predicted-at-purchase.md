# Late Delivery is predicted at purchase time

The Late Delivery model makes its prediction at checkout, so its Gold features may only use data known at purchase (Order, Order Item, Product, Seller, Customer location). Predicting at shipment time would allow richer features such as carrier pickup date, but the business can only act on a prediction (e.g. by adjusting the estimated delivery date) at checkout. Adding any feature known only after purchase leaks the label and must be rejected, even if it improves offline accuracy.
