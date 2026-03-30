#include <QDialog>
#include <QGridLayout>
#include <QPlainTextEdit>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLabel>
#include <QLineEdit>
#include <QCheckBox>
#include <QPushButton>
#include <QMenu>
#include <QAction>
#include <QInputDialog>
#include <QRegularExpression>
#include <QListWidget>
#include <QFileDialog>
#include <QJsonDocument>
#include <QJsonObject>
#include <QTimer>
#include <QMetaObject>

#include <Stockman/DBManager/DBManager.hpp>
#include <Stockman/Grpc/Client.hpp>
#include <Stockman/Stockman.hpp>
#include <Stockman/KelpieController.hpp>
#include <Stockman/AppContext.hpp>
#include <Util/Base.hpp>
#include <Util/UiConstants.hpp>

#include <UserInterface/Dialogs/Connect.hpp>
#include <UserInterface/StockmanUI.hpp>
#include <spdlog/spdlog.h>
#include <Util/Message.hpp>

namespace {
constexpr const char* kDefaultHost = StockmanNamespace::UiConstants::kDefaultHost;
constexpr const char* kDefaultPort = StockmanNamespace::UiConstants::kDefaultPort;
}

void StockmanNamespace::UserInterface::Dialogs::Connect::setupUi( QDialog* Form )
{
    this->ConnectDialog = Form;

    if ( Form->objectName().isEmpty() ) {
        Form->setObjectName( QString::fromUtf8( "Form" ) );
}

    Form->setMinimumWidth( StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogMinWidth) );
    Form->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Preferred);

    const auto style = StockmanNamespace::Util::IsDarkMode()
        ? QString::fromLatin1(StockmanNamespace::UiConstants::kStyleDark)
        : QString::fromLatin1(StockmanNamespace::UiConstants::kStyleLight);
    StockmanNamespace::Util::ApplyStyle( Form, style );

    gridLayout = new QGridLayout( Form );
    gridLayout->setObjectName( QString::fromUtf8( "gridLayout" ) );
    constexpr int margin = StockmanNamespace::UiConstants::kConnectDialogMargin;
    gridLayout->setContentsMargins( StockmanNamespace::Util::UiScalePx(margin),
                                    StockmanNamespace::Util::UiScalePx(margin),
                                    StockmanNamespace::Util::UiScalePx(margin),
                                    StockmanNamespace::Util::UiScalePx(margin) );
    gridLayout->setHorizontalSpacing( StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogHSpacing) );
    gridLayout->setVerticalSpacing( StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogVSpacing) );

    plainTextEdit = new QPlainTextEdit( Form );
    plainTextEdit->setObjectName( QString::fromUtf8( "plainTextEdit" ) );
    const int headerHeight = StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogHeaderHeight);
    plainTextEdit->setMinimumHeight(headerHeight);
    plainTextEdit->setMaximumHeight(headerHeight);
    plainTextEdit->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Fixed);
    plainTextEdit->setFrameStyle(QFrame::NoFrame);
    plainTextEdit->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    plainTextEdit->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    plainTextEdit->setReadOnly( true );
    plainTextEdit->setPlainText( "Stockman gRPC 连接向导" );

    label_Port = new QLabel( Form );
    label_Port->setObjectName( QString::fromUtf8( "label_Port" ) );

    ButtonNewProfile = new QPushButton( Form );
    ButtonNewProfile->setObjectName( QString::fromUtf8( "ButtonNewProfile" ) );
    ButtonNewProfile->setMinimumSize( QSize( StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogBtnMinWidth),
                                             StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogBtnMinHeight) ) );

    label_Name = new QLabel( Form );
    label_Name->setObjectName( QString::fromUtf8( "label_Name" ) );

    lineEdit_Name = new QLineEdit( Form );
    lineEdit_Name->setObjectName( QString::fromUtf8( "lineEdit_Name" ) );
    lineEdit_Name->setMinimumSize( QSize( StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogInputMinWidth), 0 ) );

    lineEdit_Host = new QLineEdit( Form );
    lineEdit_Host->setObjectName( QString::fromUtf8( "lineEdit_Host" ) );

    lineEdit_Port = new QLineEdit( Form );
    lineEdit_Port->setObjectName( QString::fromUtf8( "lineEdit_Port" ) );

    lineEdit_User = new QLineEdit( Form );
    lineEdit_User->setObjectName( QString::fromUtf8( "lineEdit_User" ) );

    lineEdit_Password = new QLineEdit( Form );
    lineEdit_Password->setObjectName( QString::fromUtf8( "lineEdit_Password" ) );
    lineEdit_Password->setEchoMode( QLineEdit::Normal );

    label_User = new QLabel( Form );
    label_User->setObjectName( QString::fromUtf8( "label_User" ) );

    ButtonConnect = new QPushButton( Form );
    ButtonConnect->setObjectName( QString::fromUtf8( "ButtonConnect" ) );

    label_Host = new QLabel( Form );
    label_Host->setObjectName( QString::fromUtf8( "label_Host" ) );

    label_Password = new QLabel( Form );
    label_Password->setObjectName( QString::fromUtf8( "label_Password" ) );

    label_TlsCa = new QLabel( Form );
    label_TlsCa->setObjectName( QString::fromUtf8( "label_TlsCa" ) );

    lineEdit_TlsCa = new QLineEdit( Form );
    lineEdit_TlsCa->setObjectName( QString::fromUtf8( "lineEdit_TlsCa" ) );

    checkUseTLS = new QCheckBox( Form );
    checkUseTLS->setObjectName( QString::fromUtf8( "checkUseTLS" ) );

    horizontalSpacer = new QSpacerItem( StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogSpacerWidth),
                                         StockmanNamespace::Util::UiScalePx(StockmanNamespace::UiConstants::kConnectDialogSpacerHeight),
                                         QSizePolicy::Expanding, QSizePolicy::Minimum );

    listContextMenu = new QMenu( Form );
    listContextMenu->addAction( "Remove", this, &Connect::itemRemove );
    listContextMenu->addAction( "Clear",  this, &Connect::itemsClear );

    listWidget = new QListWidget( Form );
    listWidget->setObjectName( QString::fromUtf8( "listWidget" ) );
    listWidget->setMinimumWidth(StockmanNamespace::Util::UiScalePx(190));
    listWidget->setMaximumWidth(StockmanNamespace::Util::UiScalePx(230));
    listWidget->setSpacing(StockmanNamespace::Util::UiScalePx(2));
    listWidget->setMaximumHeight(QWIDGETSIZE_MAX);
    listWidget->setStyleSheet(QStringLiteral(
        "QListWidget::item { padding: %1px %2px; min-height: %3px; }")
        .arg(StockmanNamespace::Util::UiScalePx(4))
        .arg(StockmanNamespace::Util::UiScalePx(8))
        .arg(StockmanNamespace::Util::UiScalePx(20)));
    listWidget->setContextMenuPolicy( Qt::CustomContextMenu );
    listWidget->addAction( listContextMenu->menuAction() );

    gridLayout->addWidget( ButtonNewProfile, 0, 0, 1, 1 );
    gridLayout->addWidget( listWidget,       1, 0, 8, 1 );

    gridLayout->addWidget( plainTextEdit,    0, 1, 1, 2 );
    gridLayout->addWidget( label_Name,       1, 1, 1, 1 );
    gridLayout->addWidget( lineEdit_Name,    1, 2, 1, 1 );

    gridLayout->addWidget( label_Host,       2, 1, 1, 1 );
    gridLayout->addWidget( lineEdit_Host,    2, 2, 1, 1 );

    gridLayout->addWidget( label_Port,       3, 1, 1, 1 );
    gridLayout->addWidget( lineEdit_Port,    3, 2, 1, 1 );

    gridLayout->addWidget( label_User,       4, 1, 1, 1 );
    gridLayout->addWidget( lineEdit_User,    4, 2, 1, 1 );

    gridLayout->addWidget( label_Password,   5, 1, 1, 1 );
    gridLayout->addWidget( lineEdit_Password,5, 2, 1, 1 );

    checkUseTLS->setText( "Use TLS" );
    gridLayout->addWidget( checkUseTLS,      6, 2, 1, 1 );

    label_TlsCa->setText( "TLS CA (optional):" );
    gridLayout->addWidget( label_TlsCa,      7, 1, 1, 1 );
    gridLayout->addWidget( lineEdit_TlsCa,   7, 2, 1, 1 );

    gridLayout->addItem(   horizontalSpacer, 8, 2, 1, 1 );
    gridLayout->addWidget( ButtonConnect,    9, 2, 1, 1 );
    gridLayout->setRowStretch(1, 1);
    gridLayout->setRowStretch(8, 0);

    StockmanNamespace::Util::NormalizeInteractiveControls(Form);
    const int fieldHeight = lineEdit_Name->sizeHint().height();
    ButtonNewProfile->setFixedHeight(fieldHeight);
    ButtonConnect->setFixedHeight(fieldHeight);
    ButtonNewProfile->setMinimumWidth(StockmanNamespace::Util::UiScalePx(120));
    ButtonConnect->setMinimumWidth(StockmanNamespace::Util::UiScalePx(110));

    paletteGray.setColor( QPalette::Base, Qt::gray );

    paletteWhite.setColor( QPalette::Base, Qt::white );

    Form->setWindowTitle( "Connect" );

    ButtonNewProfile->setText( "New Profile" );

    label_Name->setText( "Name:" );
    label_Host->setText( "Host:" );
    label_Port->setText( "Port:" );
    label_User->setText( "Token:" );
    label_Password->setText( "Client Name (optional):" );
    label_TlsCa->setText( "TLS CA (optional):" );

    ButtonConnect->setText( "Connect" );
    ButtonConnect->setFocus();

    // Layout + 样式全部生效后按最终 sizeHint 重新定尺寸，避免底部按钮越界/裁切。
    Form->ensurePolished();
    if (auto* layout = Form->layout())
    {
        layout->activate();
    }
    QSize finalHint = Form->sizeHint().expandedTo(Form->minimumSizeHint());
    finalHint.rheight() += StockmanNamespace::Util::UiScalePx(10);
    Form->setMinimumSize(finalHint);
    Form->resize(finalHint);

    connect( listWidget, &QListWidget::itemPressed, this, &Connect::itemSelected );
    connect( listWidget, &QListWidget::customContextMenuRequested, this, &Connect::handleContextMenu );

    connect( lineEdit_Name, &QLineEdit::returnPressed, this, [&](){
        onButton_Connect();
    } );

    connect( lineEdit_User, &QLineEdit::returnPressed, this, [&](){
        onButton_Connect();
    } );

    connect( lineEdit_Host, &QLineEdit::returnPressed, this, [&](){
        onButton_Connect();
    } );

    connect( lineEdit_Port, &QLineEdit::returnPressed, this, [&](){
        onButton_Connect();
    } );

    connect( lineEdit_Password, &QLineEdit::returnPressed, this, [&](){
        onButton_Connect();
    } );

    QMetaObject::connectSlotsByName( Form );
}

Util::ConnectionInfo StockmanNamespace::UserInterface::Dialogs::Connect::StartDialog( bool FromAction )
{
    auto ProfileName = std::string();

    listWidget->clear();

    for ( auto & KelpieConnection : KelpieList )
    {
        listWidget->addItem( KelpieConnection.Name );
    }

    listWidget->setCurrentRow( 0 );

    if ( ! listWidget->selectedItems().empty() ) {
        this->itemSelected();
	    } else
	    {
	        this->isNewProfile = true;
	        lineEdit_TlsCa->clear();
	        checkUseTLS->setChecked(false);
	    }

    // 每次显示前重新按当前字体/样式收敛几何，避免按钮与输入框高度漂移或底部越界。
    StockmanNamespace::Util::NormalizeInteractiveControls(ConnectDialog);
    const int fieldHeight = lineEdit_Name->sizeHint().height();
    ButtonNewProfile->setFixedHeight(fieldHeight);
    ButtonConnect->setFixedHeight(fieldHeight);
    ConnectDialog->ensurePolished();
    if (auto* layout = ConnectDialog->layout())
    {
        layout->activate();
    }
    QSize runHint = ConnectDialog->sizeHint().expandedTo(ConnectDialog->minimumSizeHint());
    runHint.rheight() += StockmanNamespace::Util::UiScalePx(10);
    ConnectDialog->setMinimumSize(runHint);
    ConnectDialog->resize(runHint);

    connect( ButtonConnect,    &QPushButton::clicked, this, &Connect::onButton_Connect );
    connect( ButtonNewProfile, &QPushButton::clicked, this, &Connect::onButton_NewProfile );

    ConnectDialog->exec();

    Util::ConnectionInfo connectionInfo;

    connectionInfo.Name       = lineEdit_Name->text();
    connectionInfo.Host       = lineEdit_Host->text();
    connectionInfo.Port       = lineEdit_Port->text();
    connectionInfo.Token      = lineEdit_User->text();
    connectionInfo.ClientName = lineEdit_Password->text();
    connectionInfo.TlsCa      = lineEdit_TlsCa->text();

    ProfileName = connectionInfo.Name.toStdString();

    if ( this->tryConnect )
    {
        if (context_) {
            context_->kelpieConfig = connectionInfo;
        }
        if ( this->isNewProfile ) {
            auto db = dbManager.lock();
            if ( db && !db->addKelpieInfo( connectionInfo ) ) {
                spdlog::warn( "Failed to add Kelpie profile to database" );
            }
        } else {
            spdlog::info( "Connected to profile via gRPC: {}", ProfileName );
        }
    }
    else
    {
        // 启动阶段关闭连接对话框：视为用户明确放弃打开 GUI，标记 gateGUI。
        if (!FromAction && context_) {
            spdlog::info("Startup cancelled from Connection Dialog");
            context_->gateGUI = true;
        } else {
            spdlog::info("Connection dialog dismissed without connecting");
        }
    }

    return connectionInfo;
}

void StockmanNamespace::UserInterface::Dialogs::Connect::passDB(const std::shared_ptr<StockmanNamespace::StockmanSpace::DBManager>& dbm)
{
    this->dbManager = dbm;
}

bool StockmanNamespace::UserInterface::Dialogs::Connect::connectGrpcBackend()
{
    auto& controllerPtr = context_->kelpieController;
    if ( !controllerPtr )
    {
        controllerPtr = std::make_shared<StockmanNamespace::KelpieController>(context_);
    }
    controllerPtr->SetContext(context_);

    StockmanNamespace::Grpc::ConnectionOptions options;
    options.Address    = QString( "%1:%2" ).arg( lineEdit_Host->text() ).arg( lineEdit_Port->text() ).toStdString();
    options.Token      = lineEdit_User->text().toStdString();
    options.ClientName = lineEdit_Password->text().toStdString();
    options.ServerName = lineEdit_Host->text().toStdString();

	    const QString caPath = lineEdit_TlsCa->text().trimmed();
	    options.UseTLS = checkUseTLS->isChecked() || !caPath.isEmpty();
	    if ( !caPath.isEmpty() )
	    {
	        options.CACertPath = caPath.toStdString();
	    }

    // TOFU: TLS 启用时，如果没有 CA 文件但有 Token，客户端会自动从 Token 派生证书进行验证
    // 无需用户手动确认

    QString errorMessage;
    if ( !controllerPtr->Connect( options, errorMessage ) )
    {
        const QString text = errorMessage.isEmpty()
                                 ? tr(StockmanNamespace::UiConstants::kDlgMsgConnectFailure)
                                 : errorMessage;
        StockmanNamespace::Ui::ShowError(tr(StockmanNamespace::UiConstants::kDlgTitleConnect), text);
        return false;
    }

    StockmanNamespace::Ui::ShowInfo(
        tr(StockmanNamespace::UiConstants::kDlgTitleConnect),
        tr(StockmanNamespace::UiConstants::kDlgMsgConnectSuccess)
            .arg(QString::fromStdString(options.Address)));

    auto* ui = context_ ? context_->ui : nullptr;
    if ( ui != nullptr )
    {
        QMetaObject::invokeMethod(
            ui,
            &StockmanNamespace::UserInterface::StockmanUi::BindKelpieController,
            Qt::QueuedConnection );
    }

    return true;
}

void StockmanNamespace::UserInterface::Dialogs::Connect::onButton_Connect()
{
    if ( lineEdit_Name->text().isEmpty() )
    {
        StockmanNamespace::Ui::ShowError(
            tr(StockmanNamespace::UiConstants::kDlgTitleConnect),
            tr(StockmanNamespace::UiConstants::kDlgMsgNameEmpty));
        return;
    }

    if ( lineEdit_Host->text().isEmpty() )
    {
        StockmanNamespace::Ui::ShowError(
            tr(StockmanNamespace::UiConstants::kDlgTitleConnect),
            tr(StockmanNamespace::UiConstants::kDlgMsgHostEmpty));
        return;
    }

    if ( lineEdit_Port->text().isEmpty()  )
    {
        StockmanNamespace::Ui::ShowError(
            tr(StockmanNamespace::UiConstants::kDlgTitleConnect),
            tr(StockmanNamespace::UiConstants::kDlgMsgPortEmpty));
        return;
    }

    auto db = dbManager.lock();
    if ( db && this->isNewProfile && db->checkKelpieExists( lineEdit_Name->text() ) )
    {
        StockmanNamespace::Ui::ShowWarn(
            tr(StockmanNamespace::UiConstants::kDlgTitleConnect),
            tr(StockmanNamespace::UiConstants::kDlgMsgProfileExists));
        return;
    }

    if ( !connectGrpcBackend() )
    {
        return;
    }

    this->tryConnect = true;
    this->listWidget->addItem( lineEdit_Name->text() );
    // Use accept() so exec() returns immediately and deterministically.
    this->ConnectDialog->accept();
}

void StockmanNamespace::UserInterface::Dialogs::Connect::itemSelected()
{
    auto ProfileName = listWidget->currentItem()->text();
    this->isNewProfile = false;

    for ( auto& Profile : KelpieList )
    {
        if ( Profile.Name == ProfileName )
        {
            lineEdit_Name->setPalette( paletteGray );
            lineEdit_Name->setReadOnly( true );
            lineEdit_Name->setText( Profile.Name );
            lineEdit_Host->setText( Profile.Host );
            lineEdit_Port->setText( Profile.Port );
            lineEdit_User->setText( Profile.Token );
            lineEdit_Password->setText( Profile.ClientName );
	            lineEdit_TlsCa->setText( Profile.TlsCa );
	            // TOFU: 有 Token 就启用 TLS（从 Token 派生证书）
	            checkUseTLS->setChecked( !Profile.TlsCa.isEmpty() || !Profile.Token.isEmpty() );
	        }
	    }

    ButtonConnect->setFocus();
}

void StockmanNamespace::UserInterface::Dialogs::Connect::onButton_NewProfile()
{
    this->isNewProfile = true;

    listWidget->setCurrentIndex(QModelIndex());

    lineEdit_Name->setText( "Death Star" );
    lineEdit_Name->setPalette(paletteWhite);
    lineEdit_Name->setReadOnly(false);

    lineEdit_Host->setText( kDefaultHost );
    lineEdit_Port->setText( kDefaultPort );
    lineEdit_User->clear();
	    lineEdit_Password->clear();
	    lineEdit_TlsCa->clear();
	    checkUseTLS->setChecked(false);
	}

void StockmanNamespace::UserInterface::Dialogs::Connect::handleContextMenu( const QPoint &pos )
{
    auto globalPos = listWidget->mapToGlobal( pos );
    listContextMenu->exec( globalPos );
}

void StockmanNamespace::UserInterface::Dialogs::Connect::itemRemove()
{
    for ( int i = 0; i < listWidget->selectedItems().size(); ++i )
    {
        auto *item = listWidget->takeItem( listWidget->currentRow() );

        if ( auto db = dbManager.lock() )
        {
            db->removeKelpieInfo( item->text() );
        }

        delete item;
    }

    this->onButton_NewProfile();
}

void StockmanNamespace::UserInterface::Dialogs::Connect::itemsClear()
{
    this->listWidget->clear();
    if ( auto db = dbManager.lock() )
    {
        db->removeAllKelpies();
    }
    this->onButton_NewProfile();
}
