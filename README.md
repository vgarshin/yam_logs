# Загрузка логов Яндекс.Метрики

Для загрузки логов (hits, visits) необходимо зарегистрировать свое приложение на https://oauth.yandex.ru/ после чего станет доступна информация с id приложения:
```
ID: {id}
Пароль: {password}
Callback URL: https://oauth.yandex.ru/verification_code
Время жизни токена: Не менее, чем 1 год
Дата создания: 6/30/2020
```
далее необходимо получить OAuth токен перейдя по ссылке:
```
https://oauth.yandex.ru/authorize?response_type=token&client_id={id}
```
