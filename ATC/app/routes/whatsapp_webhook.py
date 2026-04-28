from fastapi import APIRouter

router = APIRouter(prefix="/whatsapp", tags=["whatsapp"])


@router.get("/")
def webhook_test():
    return {"message": "whatsapp endpoint funcionando"}