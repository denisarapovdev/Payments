import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Response, status

from payments.api.deps import PaymentServiceDep, require_api_key
from payments.schemas.payment import (
    CreatePaymentRequest,
    PaymentAcceptedResponse,
    PaymentResponse,
)

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"description": "Invalid or missing API key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "Idempotency-Key was already used with a different body"}},
)
async def create_payment(
    body: CreatePaymentRequest,
    service: PaymentServiceDep,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAcceptedResponse:
    result = await service.create(body, idempotency_key)
    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAcceptedResponse.model_validate(result.payment)


@router.get("/{payment_id}", responses={404: {"description": "Payment not found"}})
async def get_payment(payment_id: uuid.UUID, service: PaymentServiceDep) -> PaymentResponse:
    return PaymentResponse.model_validate(await service.get(payment_id))
