from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from app.models import Response
from app.utils import submit_promotion


# Create your views here.
def hello_world(request):
    return HttpResponse(Response(0, None, "Hi Mom!").json())


def submit(request):
    pid, mid = submit_promotion(
        request.GET.get('description'),
        request.GET.get('email'),
        request.GET.get('file_path'),
    )

    body = 'Promotion Submitted [pid:{0}]'.format(pid)
    return HttpResponse(Response(0, None, body).json())
