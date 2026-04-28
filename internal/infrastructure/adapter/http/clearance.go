package http

import (
	"net/http"
	"encoding/json"	

	"github.com/gorilla/mux"

	"github.com/go-clearance/shared/erro"
	"github.com/go-clearance/internal/domain/model"

	"go.opentelemetry.io/otel/codes"
)

// About add payment
func (h *HttpRouters) AddPayment(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "AddPayment")
	defer cancel()
	defer span.End()
	
	// decode payload		
	payment := model.Payment{}
	err := json.NewDecoder(req.Body).Decode(&payment)
	defer req.Body.Close()
    
	if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return h.ErrorHandler(h.getTraceID(ctx), erro.ErrBadRequest)
    }

	res, err := h.workerService.AddPayment(ctx, &payment)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusCreated, res)
}

// About get payment
func (h *HttpRouters) GetPayment(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "GetPayment")
	defer cancel()
	defer span.End()

	// decode payload			
	vars := mux.Vars(req)
	PaymentID, err := h.parseIDParam(vars)
    if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return h.ErrorHandler(h.getTraceID(ctx), err)
    }

	payment := model.Payment{ID: PaymentID}

	res, err := h.workerService.GetPayment(ctx, &payment)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}

// About get payment from order
func (h *HttpRouters) GetPaymentFromOrder(rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel, span := h.withContext(req, "GetPaymentFromOrder")
	defer cancel()
	defer span.End()

	// decode payload			
	vars := mux.Vars(req)
	OrderID, err := h.parseIDParam(vars)
    if err != nil {
		span.RecordError(err) 
		span.SetStatus(codes.Error, err.Error())
		return h.ErrorHandler(h.getTraceID(ctx), err)
    }

	order := model.Order{ID: OrderID}

	res, err := h.workerService.GetPaymentFromOrder(ctx, &order)
	if err != nil {
		return h.ErrorHandler(h.getTraceID(ctx), err)
	}
	
	return h.writeJSON(rw, http.StatusOK, res)
}
