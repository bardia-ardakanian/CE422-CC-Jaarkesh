import json
import logging
import time

from app.globals import ErrorCodes
from bson import json_util, ObjectId
from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from threading import Thread
from app.models import State, Response
from app.utils import submit, get_promotion_by_id, handle_uploaded_file, generate_name
from app.utils import mq_make_queue, mq_close_connection, mq_get_channel, mq_get_queued_messages_count, mq_consume, \
    mq_make_connection


# Create your views here.
def hello_world(request):
    return HttpResponse(Response(0, None, "Hi Mom!").json())


@csrf_exempt
def submit_promotion(request):
    error_code = ErrorCodes.DEFAULT.value
    error_message = ErrorCodes.DEFAULT.name
    body = ''

    try:
        pid, mid = submit(
            request.GET.get('description', ''),
            request.GET.get('email', ''),
            handle_uploaded_file(request.FILES['image'], generate_name(4)),
        )

        body = 'Promotion Submitted [pid:{0}]'.format(pid)
    except Exception as e:
        error_code = ErrorCodes.SUBMISSION_FAILED.value
        error_message = ErrorCodes.SUBMISSION_FAILED.name
        logging.exception(e)

    return HttpResponse(Response(error_code, error_message, body).json())


def get_promotion(request):
    error_code = ErrorCodes.DEFAULT.value
    error_message = ErrorCodes.DEFAULT.name
    body = ''

    try:
        promotion = get_promotion_by_id(request.GET.get('_pid'))
        state = State(promotion['state'])

        if state == State.PROCESSING:
            body = 'Promotion [{0}] is in processing queue'.format(request.GET.get('_pid'))
        elif state == State.ACCEPTED:
            info = {
                'Message': 'Promotion [{0}] has been ACCEPTED'.format(request.GET.get('_pid')),
                'Promotion': promotion,
            }
            body = json.loads(json_util.dumps(info, indent=2))
        elif state == State.REJECTED:
            body = 'Promotion [{0}] has been REJECTED.'.format(request.GET.get('_pid'))
        else:
            body = 'There has been a problem in our service. Please try again later!'
    except Exception as e:
        error_code = ErrorCodes.INFORMATION_RETRIEVAL_FAILED.value
        error_message = ErrorCodes.INFORMATION_RETRIEVAL_FAILED.name
        logging.exception(e)

    return HttpResponse(Response(error_code, error_message, body).json())


def classify_and_notify(request):
    conn = mq_make_connection()
    channel = mq_get_channel(conn)

    # thread = Thread(target=mq_consume, args=[channel])
    # thread.start()
    mq_consume(channel)

    # _conn = mq_make_connection()
    # _channel = mq_get_channel(conn)
    #
    # while mq_get_queued_messages_count(_channel) > 0:  # Busy waiting
    #     time.sleep(1)
    #
    # mq_close_connection(_conn)
    # thread.join()

    mq_close_connection(conn)
