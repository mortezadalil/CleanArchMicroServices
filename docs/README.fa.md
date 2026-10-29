# MiniShop — راهنمای سریع فارسی

> 📚 دورهٔ کامل آموزشی (۱۷ ماژول، تمرین، برنامهٔ کلاس): [course/MiniShop-Microservices-Course.fa.md](course/MiniShop-Microservices-Course.fa.md)

MiniShop یک فروشگاه بسیار ساده (سه محصول و یک کیف پول) است که پشت دکمهٔ «ثبت سفارش» آن یک جریان
واقعی میکروسرویسی اجرا می‌شود: **Saga**، **Transactional Outbox**، **Inbox** برای حذف پیام تکراری،
**CQRS** با دیتابیس خواندنی جدا و **RabbitMQ**. حالت توسعه‌دهنده (Developer Mode) همان چیزی را
نشان می‌دهد که واقعاً در سرویس‌ها اتفاق افتاده است؛ هیچ انیمیشن یا عدد ساختگی وجود ندارد.

## اجرا

```bash
git clone https://github.com/mortezadalil/CleanArchMicroServices.git
cd CleanArchMicroServices
docker compose -f infra/compose.yaml up --build
```

* فروشگاه و کنسول توسعه‌دهنده: http://localhost:5100 (مسیر `/developer`)
* پنل RabbitMQ: http://localhost:15672 (کاربر و رمز `guest`)

اگر می‌خواهید سرویس‌ها را با `dotnet run` اجرا کنید: `docker compose -f infra/compose.yaml up -d rabbitmq`
و سپس `scripts/run-local.sh` (نیازمند .NET 10 SDK).

### اجرا و دیباگ بدون داکر (JetBrains Rider)

پیش‌نیاز: .NET 10 SDK و RabbitMQ نصب‌شده روی خود سیستم. سرویس‌ها فقط از طریق broker با هم حرف
می‌زنند، پس RabbitMQ حتماً لازم است. روی مک:

```bash
brew install rabbitmq
brew services start rabbitmq
rabbitmq-plugins enable rabbitmq_management
```

فایل `MiniShop.slnx` را در Rider باز کنید. پیکربندی‌های اجرای مشترک پوشهٔ `.run/` در فهرست Run
زیر پوشهٔ **MiniShop** دیده می‌شوند: یکی برای هر سرویس و یک پیکربندی ترکیبی به نام
**MiniShop (all services)**. همین را انتخاب کنید و **Debug** بزنید تا هر چهار سرویس با دیباگر بالا
بیایند. سپس http://localhost:5100 را باز کنید. سرویس‌های Orders، Inventory و Payments روی `/` صفحه‌ای
ندارند و خطای ۴۰۴ آن‌جا طبیعی است؛ برای بررسی سلامت از `/health/ready` استفاده کنید.

بدون داکر، سناریوی S09 فقط bus سرویس Orders را دوباره راه می‌اندازد و برای S04 خودتان RabbitMQ را با
`brew services stop rabbitmq` متوقف و با `brew services start rabbitmq` دوباره روشن کنید.

## چهار پروژه

| پروژه | مسئولیت | دیتابیس |
|---|---|---|
| Orders | API ثبت سفارش، وضعیت سفارش، Saga و timeoutها | `orders.db` |
| Inventory | کاتالوگ، موجودی، رزرو/آزادسازی/مصرف | `inventory.db` |
| Payments | کیف پول و دفتر کل (حداکثر یک برداشت برای هر سفارش) | `payments.db` |
| Web | فروشگاه Blazor، projectionها، کنسول توسعه‌دهنده، آزمایشگاه سناریو | `readmodel.db` |

پوشه‌های `contracts/` و `shared/` کد مشترک linked-source هستند (جایگزین آموزشی یک پکیج NuGet) و پروژهٔ پنجم نیستند.

## جریان سفارش

1. Web با هدر `Idempotency-Key` به `POST /api/orders` درخواست می‌دهد و فوراً `202` می‌گیرد.
2. Orders سفارش، نمونهٔ Saga و رویداد `OrderPlaced` را در **یک تراکنش** و در جدول Outbox ذخیره می‌کند.
3. Dispatcher پیام را به RabbitMQ می‌فرستد؛ Saga فرمان `ReserveStock` را صادر می‌کند.
4. Inventory در یک تراکنش رزرو می‌کند (Inbox + موجودی + رزرو + رویداد `StockReserved` در Outbox).
5. Saga فرمان `ChargeWallet` می‌دهد؛ Payments فقط یک بار برداشت می‌کند و `PaymentCompleted` منتشر می‌کند.
6. Saga رویداد `OrderConfirmed` را منتشر می‌کند و Inventory رزرو را به مصرف واقعی تبدیل می‌کند.
7. Web رویدادها را در `readmodel.db` می‌نشاند و صفحه از طریق SignalR داخلی Blazor به‌روز می‌شود.

**نکتهٔ مهم:** timeout پرداخت به معنی «پرداخت نشده» نیست. Saga ابتدا `CancelPayment` می‌فرستد؛ Payments یا
`PaymentAlreadyCompleted` برمی‌گرداند (سفارش تأیید می‌شود) یا یک tombstone در دفتر کل ثبت می‌کند تا
برداشت دیرهنگام هرگز ممکن نشود و بعد موجودی آزاد می‌شود.

## چرا Web درخواست snapshot می‌دهد؟

RabbitMQ هر event را فقط به صف‌هایی می‌رساند که **در لحظهٔ انتشار** وجود دارند. در اولین اجرای Compose،
Inventory و Payments کاتالوگ و کیف پول‌ها را قبل از ساخته‌شدن صف Web منتشر می‌کنند و broker آن‌ها را دور
می‌ریزد (broker انبار رویداد نیست). به همین دلیل `ReadModelBootstrapper` در Web تا وقتی read model خالی است
هر ۵ ثانیه رویداد `ReadModelSnapshotRequested` منتشر می‌کند و سرویس‌های مالک داده‌ها را از طریق Outbox دوباره
اعلام می‌کنند. projectionها idempotent هستند، پس پاسخ تکراری بی‌ضرر است.

## سناریوها (S01 تا S16)

از منوی بالای Developer Mode یک سناریو انتخاب و **Run** را بزنید. هر سناریو خودش کیف پول و خطای
لازم را آماده می‌کند، سفارش می‌دهد، روی وضعیت **پایدار** در دیتابیس سرویس مالک صبر می‌کند و در پایان
invariantها را با شواهد نشان می‌دهد (`PASSED` / `FAILED` / `INCONCLUSIVE`). کد هر سناریو در
`src/MiniShop.Web/Scenarios/ScenarioCatalog.cs` است و مثل یک تست خوانده می‌شود.

سناریوی **S04 (قطع RabbitMQ)** نیاز دارد کسی broker را متوقف کند: یا پروفایل `faults` را بالا بیاورید
(`docker compose -f infra/compose.yaml --profile faults up`) تا کانتینر runner این کار را بکند، یا وقتی لاگ
اجرا خواست، دستی `docker compose stop rabbitmq` و سپس `docker compose start rabbitmq` را اجرا کنید.

## بازگشت به حالت اولیه

دکمهٔ **⟲ Reset data** در هدر (فقط در Development) پس از تأیید، همهٔ قوانین fault را حذف می‌کند، صف‌های
MiniShop در RabbitMQ (از جمله `*_error`) را خالی می‌کند، جدول‌های هر چهار دیتابیس را پاک می‌کند و دادهٔ
اولیه را برمی‌گرداند: سه محصول با ۱۰ عدد موجودی و کیف پول `demo-customer` با ۲٬۰۰۰٬۰۰۰ تومان.

## تست‌ها

```bash
dotnet test MiniShop.slnx --filter "FullyQualifiedName!~MiniShop.E2E"   # واحد و یکپارچه، بدون زیرساخت
dotnet test tests/MiniShop.E2E                                           # روی stack در حال اجرا
```

## محدودیت‌ها

* دموی آموزشی است: SQLite تک‌نویسنده است و هر سرویس یک نمونه دارد؛ برای نمایش scale-out چندنودی مناسب نیست.
* Control plane و endpointهای تشخیصی فقط در Development فعال‌اند و هدر `X-Demo-Token` می‌خواهند.
* فایل Compose در محیطی که این پروژه ساخته شد قابل اجرا نبود (داکر در دسترس نبود)؛ اما همهٔ سرویس‌ها،
  شانزده سناریو و تست‌ها با RabbitMQ واقعی اجرا و تأیید شده‌اند. اگر در Compose مشکلی دیدید، خروجی
  `docker compose -f infra/compose.yaml logs` را در یک issue بگذارید.

راهنمای کامل انگلیسی: [README.md](../README.md)
