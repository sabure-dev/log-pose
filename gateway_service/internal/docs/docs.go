package docs

import (
	"net/http"
)

const OpenAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "Log Pose API Gateway",
    "description": "Unified perimeter API Gateway for Log Pose polyglot microservices system. Provides authentication, reverse proxying to order_service, synchronous gRPC querying to inventory_service, and distributed tracing.",
    "version": "1.0.0"
  },
  "servers": [
    {
      "url": "/",
      "description": "Current Gateway Host"
    }
  ],
  "tags": [
    {
      "name": "Auth",
      "description": "User registration and JWT token issuance"
    },
    {
      "name": "Admin",
      "description": "Privileged user and role provisioning (Admin role required)"
    },
    {
      "name": "Inventory",
      "description": "Warehouse catalog and stock availability via gRPC"
    },
    {
      "name": "Orders",
      "description": "Order lifecycle management proxied to order_service"
    },
    {
      "name": "Health",
      "description": "Gateway health checks"
    }
  ],
  "paths": {
    "/health": {
      "get": {
        "tags": ["Health"],
        "summary": "Gateway health check",
        "responses": {
          "200": {
            "description": "Service is healthy",
            "content": {
              "application/json": {
                "example": {"status": "ok", "service": "gateway_service"}
              }
            }
          }
        }
      }
    },
    "/api/v1/auth/register": {
      "post": {
        "tags": ["Auth"],
        "summary": "Register a new customer account",
        "description": "Registers a new customer and returns a signed JWT token. Public registration is strictly restricted to regular customers; privileged roles cannot be self-registered.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/RegisterRequest"
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "User created successfully",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/AuthResponse"
                }
              }
            }
          },
          "400": { "description": "Invalid input" },
          "403": { "description": "Self-registration with privileged roles is forbidden" },
          "409": { "description": "User already exists" }
        }
      }
    },
    "/api/v1/admin/users": {
      "post": {
        "tags": ["Admin"],
        "summary": "Provision a privileged user account (Admin only)",
        "description": "Creates a new warehouse operator or admin account. Strictly requires an authenticated user with admin role.",
        "security": [
          { "BearerAuth": [] }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/RegisterRequest"
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Privileged user created successfully",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/AuthResponse"
                }
              }
            }
          },
          "400": { "description": "Invalid input or unsupported role" },
          "401": { "description": "Missing or invalid admin token" },
          "403": { "description": "Insufficient permissions (admin role required)" },
          "409": { "description": "User already exists" }
        }
      }
    },
    "/api/v1/auth/login": {
      "post": {
        "tags": ["Auth"],
        "summary": "Authenticate user",
        "description": "Authenticates user credentials and returns a signed JWT token. Pre-seeded demo accounts: ` + "`" + `operator` + "`" + ` / ` + "`" + `operatorpass123` + "`" + `, ` + "`" + `admin` + "`" + ` / ` + "`" + `adminpass123` + "`" + `.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/LoginRequest"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Login successful",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/AuthResponse"
                }
              }
            }
          },
          "401": { "description": "Invalid username or password" }
        }
      }
    },
    "/api/v1/inventory/products": {
      "get": {
        "tags": ["Inventory"],
        "summary": "List warehouse catalog products",
        "description": "Queries inventory_service via synchronous gRPC with pagination.",
        "parameters": [
          {
            "name": "limit",
            "in": "query",
            "schema": { "type": "integer", "default": 50 }
          },
          {
            "name": "offset",
            "in": "query",
            "schema": { "type": "integer", "default": 0 }
          }
        ],
        "responses": {
          "200": {
            "description": "Catalog products retrieved",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ListProductsResponse"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/inventory/products/{id}": {
      "get": {
        "tags": ["Inventory"],
        "summary": "Get product stock by UUID",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": { "type": "string", "format": "uuid" }
          }
        ],
        "responses": {
          "200": {
            "description": "Product details and stock",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ProductStock"
                }
              }
            }
          },
          "404": { "description": "Product not found" }
        }
      }
    },
    "/api/v1/inventory/check": {
      "post": {
        "tags": ["Inventory"],
        "summary": "Pre-flight stock availability check",
        "description": "Verifies stock availability and expected price against the warehouse catalog.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/CheckStockRequest"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Stock check completed",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/CheckStockResponse"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/orders/": {
      "post": {
        "tags": ["Orders"],
        "summary": "Create a new order",
        "security": [{ "BearerAuth": [] }],
        "description": "Proxied to order_service with verified X-User-Id. Executes synchronous gRPC pre-flight validation against inventory_service before saving.",
        "parameters": [
          {
            "name": "Idempotency-Key",
            "in": "header",
            "required": false,
            "schema": { "type": "string" }
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/OrderCreate"
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Order created in pending state",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/OrderRead" }
              }
            }
          },
          "400": { "description": "Invalid quantity or product not found" },
          "409": { "description": "Insufficient stock, price mismatch, or idempotency conflict" },
          "401": { "description": "Unauthorized - Missing or invalid JWT" }
        }
      },
      "get": {
        "tags": ["Orders"],
        "summary": "List orders for the authenticated user",
        "security": [{ "BearerAuth": [] }],
        "parameters": [
          {
            "name": "skip",
            "in": "query",
            "schema": { "type": "integer", "default": 0 }
          },
          {
            "name": "limit",
            "in": "query",
            "schema": { "type": "integer", "default": 100 }
          }
        ],
        "responses": {
          "200": {
            "description": "List of user orders",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": { "$ref": "#/components/schemas/OrderRead" }
                }
              }
            }
          },
          "401": { "description": "Unauthorized" }
        }
      }
    },
    "/api/v1/orders/{order_id}": {
      "get": {
        "tags": ["Orders"],
        "summary": "Get order by ID",
        "security": [{ "BearerAuth": [] }],
        "parameters": [
          {
            "name": "order_id",
            "in": "path",
            "required": true,
            "schema": { "type": "string", "format": "uuid" }
          }
        ],
        "responses": {
          "200": {
            "description": "Order details",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/OrderRead" }
              }
            }
          },
          "404": { "description": "Order not found" }
        }
      }
    },
    "/api/v1/orders/{order_id}/cancel": {
      "post": {
        "tags": ["Orders"],
        "summary": "Cancel an order (Customer)",
        "security": [{ "BearerAuth": [] }],
        "parameters": [
          {
            "name": "order_id",
            "in": "path",
            "required": true,
            "schema": { "type": "string", "format": "uuid" }
          }
        ],
        "responses": {
          "200": { "description": "Order cancelled" },
          "404": { "description": "Order not found" },
          "409": { "description": "Cannot cancel order in current state" }
        }
      }
    },
    "/api/v1/orders/{order_id}/ship": {
      "post": {
        "tags": ["Orders"],
        "summary": "Mark order ready for shipping (Warehouse Operator only)",
        "security": [{ "BearerAuth": [] }],
        "description": "Requires warehouse_operator or admin role. Automatically injects X-Internal-Token.",
        "parameters": [
          {
            "name": "order_id",
            "in": "path",
            "required": true,
            "schema": { "type": "string", "format": "uuid" }
          }
        ],
        "responses": {
          "200": { "description": "Order transitioned to ready_for_shipping" },
          "403": { "description": "Forbidden - Operator role required" },
          "409": { "description": "Invalid transition" }
        }
      }
    },
    "/api/v1/orders/{order_id}/complete": {
      "post": {
        "tags": ["Orders"],
        "summary": "Complete order delivery (Warehouse Operator only)",
        "security": [{ "BearerAuth": [] }],
        "description": "Permanently confirms stock deduction in inventory. Requires warehouse_operator or admin role.",
        "parameters": [
          {
            "name": "order_id",
            "in": "path",
            "required": true,
            "schema": { "type": "string", "format": "uuid" }
          }
        ],
        "responses": {
          "200": { "description": "Order completed" },
          "403": { "description": "Forbidden - Operator role required" }
        }
      }
    },
    "/api/v1/orders/maintenance/requeue-parked": {
      "post": {
        "tags": ["Orders"],
        "summary": "Requeue parked outbox events (Operator maintenance)",
        "security": [{ "BearerAuth": [] }],
        "responses": {
          "200": { "description": "Parked events reset for retry" },
          "403": { "description": "Forbidden" }
        }
      }
    }
  },
  "components": {
    "securitySchemes": {
      "BearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "Enter the JWT token obtained from /api/v1/auth/login or /api/v1/auth/register"
      }
    },
    "schemas": {
      "RegisterRequest": {
        "type": "object",
        "required": ["username", "password"],
        "properties": {
          "username": { "type": "string", "example": "customer1" },
          "password": { "type": "string", "example": "password123" },
          "role": { "type": "string", "enum": ["customer", "warehouse_operator", "admin"], "default": "customer" }
        }
      },
      "LoginRequest": {
        "type": "object",
        "required": ["username", "password"],
        "properties": {
          "username": { "type": "string", "example": "operator" },
          "password": { "type": "string", "example": "operatorpass123" }
        }
      },
      "AuthResponse": {
        "type": "object",
        "properties": {
          "token": { "type": "string" },
          "user_id": { "type": "string", "format": "uuid" },
          "username": { "type": "string" },
          "role": { "type": "string" }
        }
      },
      "ProductStock": {
        "type": "object",
        "properties": {
          "product_id": { "type": "string", "format": "uuid" },
          "name": { "type": "string" },
          "price": { "type": "string" },
          "available": { "type": "integer" },
          "reserved": { "type": "integer" }
        }
      },
      "ListProductsResponse": {
        "type": "object",
        "properties": {
          "products": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/ProductStock" }
          },
          "total": { "type": "integer" }
        }
      },
      "CheckStockItem": {
        "type": "object",
        "required": ["product_id", "quantity"],
        "properties": {
          "product_id": { "type": "string", "format": "uuid", "example": "3fa85f64-5717-4562-b3fc-2c963f66afa6" },
          "quantity": { "type": "integer", "example": 2 },
          "expected_price": { "type": "string", "example": "1500.50" }
        }
      },
      "CheckStockRequest": {
        "type": "object",
        "required": ["items"],
        "properties": {
          "items": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/CheckStockItem" }
          }
        }
      },
      "CheckStockResponse": {
        "type": "object",
        "properties": {
          "eligible": { "type": "boolean" },
          "overall_status": { "type": "string", "example": "OK" },
          "reason": { "type": "string" }
        }
      },
      "OrderItemCreate": {
        "type": "object",
        "required": ["product_id", "quantity", "price"],
        "properties": {
          "product_id": { "type": "string", "format": "uuid", "example": "3fa85f64-5717-4562-b3fc-2c963f66afa6" },
          "quantity": { "type": "integer", "example": 2 },
          "price": { "type": "string", "example": "1500.50" }
        }
      },
      "OrderCreate": {
        "type": "object",
        "required": ["items"],
        "properties": {
          "items": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/OrderItemCreate" }
          },
          "shipping_address": { "type": "string", "example": "42 Cloud Avenue, Tokyo" }
        }
      },
      "OrderRead": {
        "type": "object",
        "properties": {
          "id": { "type": "string", "format": "uuid" },
          "user_id": { "type": "string", "format": "uuid" },
          "status": { "type": "string", "example": "pending" },
          "total_amount": { "type": "string", "example": "3001.00" },
          "shipping_address": { "type": "string" },
          "cancellation_reason": { "type": "string", "nullable": true },
          "created_at": { "type": "string", "format": "date-time" },
          "updated_at": { "type": "string", "format": "date-time" }
        }
      }
    }
  }
}`

const SwaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Log Pose API Gateway - Swagger UI</title>
  <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" >
  <style>
    html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
    *, *:before, *:after { box-sizing: inherit; }
    body { margin:0; background: #fafafa; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    .topbar { display: none !important; }
    .swagger-ui .info { margin: 25px 0; }
    .swagger-ui .info .title { font-size: 32px; color: #1e293b; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
  window.onload = function() {
    window.ui = SwaggerUIBundle({
      url: "/api/v1/openapi.json",
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIStandalonePreset
      ],
      layout: "BaseLayout",
      persistAuthorization: true
    });
  };
  </script>
</body>
</html>`

func OpenAPIJSONHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(OpenAPISpec))
}

func SwaggerUIHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(SwaggerUIHTML))
}
