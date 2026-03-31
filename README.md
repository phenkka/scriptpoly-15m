docker compose up --build collect_polymarket - для запуска сервиса для сбора с полимаркета
docker compose up --build collect_predict - для запуска сервиса по сбора с предидиктион фан

Что бы запустить весь проект - docker compose up -d --build
Посмотреть логи - docker compose logs -f (collect_predict/collect_polymarket/analyzer)
Вычленить профит из сервиса по анализу - docker compose logs -f analyzer | grep ARBITRAGE