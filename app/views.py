from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from app.models import Response


# Create your views here.
def hello_world(request):
    return HttpResponse(Response(0, None, "Hi Mom!").json())
