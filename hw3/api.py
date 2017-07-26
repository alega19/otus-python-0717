#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Нужно реализовать простое HTTP API сервиса скоринга. Шаблон уже есть в api.py, тесты в test.py.
# API необычно тем, что польщователи дергают методы POST запросами. Чтобы получить результат
# пользователь отправляет в POST запросе валидный JSON определенного формата на локейшн /method

# Структура json-запроса:

# {"account": "<имя компании партнера>", "login": "<имя пользователя>", "method": "<имя метода>",
#  "token": "<аутентификационный токен>", "arguments": {<словарь с аргументами вызываемого метода>}}

# account - строка, опционально, может быть пустым
# login - строка, обязательно, может быть пустым
# method - строка, обязательно, может быть пустым
# token - строка, обязательно, может быть пустым
# arguments - словарь (объект в терминах json), обязательно, может быть пустым

# Валидация:
# запрос валиден, если валидны все поля по отдельности

# Структура ответа:
# {"code": <числовой код>, "response": {<ответ вызываемого метода>}}
# {"code": <числовой код>, "error": {<сообщение об ошибке>}}

# Аутентификация:
# смотри check_auth в шаблоне. В случае если не пройдена, нужно возвращать
# {"code": 403, "error": "Forbidden"}

# Метод online_score.
# Аргументы:
# phone - строка или число, длиной 11, начинается с 7, опционально, может быть пустым
# email - строка, в которой есть @, опционально, может быть пустым
# first_name - строка, опционально, может быть пустым
# last_name - строка, опционально, может быть пустым
# birthday - дата в формате DD.MM.YYYY, с которой прошло не больше 70 лет, опционально, может быть пустым
# gender - число 0, 1 или 2, опционально, может быть пустым

# Валидация аругементов:
# аргументы валидны, если валидны все поля по отдельности и если присутсвует хоть одна пара
# phone-email, first name-last name, gender-birthday с непустыми значениями.

# Контекст
# в словарь контекста должна прописываться запись  "has" - список полей,
# которые были не пустые для данного запроса

# Ответ:
# в ответ выдается произвольное число, которое больше или равно 0
# {"score": <число>}
# или если запрос пришел от валидного пользователя admin
# {"score": 42}
# или если произошла ошибка валидации
# {"code": 422, "error": "<сообщение о том какое поле невалидно>"}


# $ curl -X POST  -H "Content-Type: application/json" -d '{"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "55cc9ce545bcd144300fe9efc28e65d415b923ebb6be1e19d2750a2c03e80dd209a27954dca045e5bb12418e7d89b6d718a9e35af34e14e1d5bcd5a08f21fc95", "arguments": {"phone": "79175002040", "email": "stupnikov@otus.ru", "first_name": "Стансилав", "last_name": "Ступников", "birthday": "01.01.1990", "gender": 1}}' http://127.0.0.1:8080/method/
# -> {"code": 200, "response": {"score": 5.0}}

# Метод clients_interests.
# Аргументы:
# client_ids - массив числе, обязательно, не пустое
# date - дата в формате DD.MM.YYYY, опционально, может быть пустым

# Валидация аругементов:
# аргументы валидны, если валидны все поля по отдельности.

# Контекст
# в словарь контекста должна прописываться запись  "nclients" - количество id'шников,
# переденанных в запрос


# Ответ:
# в ответ выдается словарь <id клиента>:<список интересов>. Список генерировать произвольно.
# {"client_id1": ["interest1", "interest2" ...], "client2": [...] ...}
# или если произошла ошибка валидации
# {"code": 422, "error": "<сообщение о том какое поле невалидно>"}

# $ curl -X POST  -H "Content-Type: application/json" -d '{"account": "horns&hoofs", "login": "admin", "method": "clients_interests", "token": "d3573aff1555cd67dccf21b95fe8c4dc8732f33fd4e32461b7fe6a71d83c947688515e36774c00fb630b039fe2223c991f045f13f24091386050205c324687a0", "arguments": {"client_ids": [1,2,3,4], "date": "20.07.2017"}}' http://127.0.0.1:8080/method/
# -> {"code": 200, "response": {"1": ["books", "hi-tech"], "2": ["pets", "tv"], "3": ["travel", "music"], "4": ["cinema", "geek"]}}

# Требование: в результате в git должно быть только два(2!) файлика: api.py, test.py.
# Deadline: следующее занятие

import abc
import json
import random
import datetime
import logging
import hashlib
import uuid
from optparse import OptionParser
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler


SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}


class Field(object):

    def __init__(self, required, nullable):
        self.required = required
        self.nullable = nullable

    def is_valid(self, value):
        if value or isinstance(value, (int, long)):
            return self._is_valid(value)
        else:
            return self.nullable


class CharField(Field):

    @staticmethod
    def _is_valid(value):
        return isinstance(value, str)


class ArgumentsField(Field):

    @staticmethod
    def _is_valid(value):
        return True


class EmailField(CharField):

    @staticmethod
    def _is_valid(value):
        return '@' in value


class PhoneField(Field):

    @staticmethod
    def _is_valid(value):
        if isinstance(value, (int, long)):
            s = str(value)
        elif isinstance(value, str):
            s = value
            try:
                int(value)
            except ValueError:
                return False
        else:
            return False
        return len(s)==11 and s[0]=='7'


class DateField(Field):

    @staticmethod
    def _is_valid(value):
        try:
            datetime.datetime.strptime(value, '%d.%m.%Y')
            return True
        except ValueError:
            return False


class BirthDayField(Field):

    @staticmethod
    def _is_valid(value):
        try:
            bday = datetime.datetime.strptime(value, '%d.%m.%Y')
            _70years = datetime.timedelta(days=70*365.25)
            return (datetime.datetime.now() - bday) < _70years
        except ValueError:
            return False


class GenderField(Field):

    @staticmethod
    def _is_valid(value):
        return value in GENDERS


class ClientIDsField(Field):

    @staticmethod
    def _is_valid(value):
        return isinstance(value, list) and all(isinstance(e, (int, long)) for e in value)


class MyMetaClass(type):  # не придумал название

    def __new__(cls, name, bases, attrs):
        fields = {fn: attrs[fn] for fn in attrs if isinstance(attrs[fn], Field)}
        attrs['fields'] = fields
        return super(MyMetaClass, cls).__new__(cls, name, bases, attrs)


class Request(object):

    __metaclass__ = MyMetaClass

    def __init__(self, data):
        self.init_fields = data
        self._invalid_fields = None

    def is_valid(self):
        self._invalid_fields = []
        for field_name in self.fields:
            field = self.fields[field_name]
            if field_name in self.init_fields:
                value = self.init_fields[field_name]
                if not field.is_valid(value):
                    self._invalid_fields.append(field_name)
            else:
                if field.required:
                    self._invalid_fields.append(field_name)
        return not self._invalid_fields

    def invalid_fields(self):
        return self._invalid_fields


class ClientsInterestsRequest(Request):
    client_ids = ClientIDsField(required=True, nullable=False)
    date = DateField(required=False, nullable=True)

    def get_client_ids(self):
        return self.init_fields['client_ids']


class OnlineScoreRequest(Request):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    def is_valid(self):
        if not super(OnlineScoreRequest, self).is_valid():
            return False
        nonempty_fields_ = set(self.nonempty_fields())
        if 'phone' in nonempty_fields_ and 'email' in nonempty_fields_:
            return True
        elif 'first_name' in nonempty_fields_ and 'last_name' in nonempty_fields_:
            return True
        elif 'birthday' in nonempty_fields_ and 'gender' in nonempty_fields_:
            return True
        else:
            return False

    def nonempty_fields(self):
        return [fn for fn in self.init_fields if self.init_fields[fn] or isinstance(self.init_fields[fn], (int, long))]


class MethodRequest(Request):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    arguments = ArgumentsField(required=True, nullable=True)
    method = CharField(required=True, nullable=False)

    def __init__(self, data):
        super(MethodRequest, self).__init__(data)
        self.account = data.get('account')
        self.login = data.get('login')
        self.token = data.get('token')
        self.arguments = data.get('arguments')
        self.method = data.get('method')

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


def check_auth(request):
    if request.login == ADMIN_LOGIN:
        digest = hashlib.sha512(datetime.datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT).hexdigest()
    else:
        digest = hashlib.sha512(request.account + request.login + SALT).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx):
    response, code = None, None
    method_request = MethodRequest(request['body'])
    if not method_request.is_valid():
        code = INVALID_REQUEST
        response = 'Invalid fields %s' % method_request.invalid_fields()
    elif not check_auth(method_request):
        code = FORBIDDEN
    else:
        if method_request.method == 'online_score':
            os_request = OnlineScoreRequest(method_request.arguments)
            if os_request.is_valid():
                code = OK
                score = 42 if method_request.is_admin else 19
                response = {'score': score}
                ctx['has'] = os_request.nonempty_fields()
            else:
                code = INVALID_REQUEST
                response = 'Invalid fields %s' % os_request.invalid_fields()
        elif method_request.method == 'clients_interests':
            ci_request = ClientsInterestsRequest(method_request.arguments)
            if ci_request.is_valid():
                code = OK
                response = {cid: ['books', 'music'] for cid in ci_request.get_client_ids()}
                ctx['nclients'] = len(ci_request.get_client_ids())
            else:
                code = INVALID_REQUEST
                response = 'Invalid fields %s' % ci_request.invalid_fields()
        else:
            code = NOT_FOUND
    return response, code


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {
        "method": method_handler
    }

    def get_request_id(self, headers):
        return headers.get('HTTP_X_REQUEST_ID', uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            if path in self.router:
                try:
                    response, code = self.router[path]({"body": request, "headers": self.headers}, context)
                except Exception, e:
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return

if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
