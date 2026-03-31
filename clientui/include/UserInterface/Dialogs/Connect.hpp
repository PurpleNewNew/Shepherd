#ifndef STOCKMAN_CONNECTDIALOG_H
#define STOCKMAN_CONNECTDIALOG_H

#include <QDialog>
#include <QGridLayout>
#include <QLineEdit>
#include <QListWidget>
#include <QPlainTextEdit>
#include <QCheckBox>
#include <QPushButton>
#include <QSpacerItem>
#include <QLabel>
#include <QStringList>
#include <vector>
#include <memory>

#include <Stockman/AppContext.hpp>

class StockmanNamespace::UserInterface::Dialogs::Connect : public QDialog
{
private:
    QGridLayout*    gridLayout;

    QPlainTextEdit* plainTextEdit;
    QLabel*         label_Name;
    QLabel*         label_Host;
    QLabel*         label_Port;
    QLabel*         label_User;
    QLabel*         label_Password;
    QLabel*         label_TlsCa;

    QLineEdit*      lineEdit_User;
    QLineEdit*      lineEdit_Password;
    QLineEdit*      lineEdit_Host;
    QLineEdit*      lineEdit_Name;
    QLineEdit*      lineEdit_Port;
    QLineEdit*      lineEdit_TlsCa;

	    QPushButton*    ButtonNewProfile;
	    QPushButton*    ButtonConnect;
	    QCheckBox*      checkUseTLS;

	    QSpacerItem*    horizontalSpacer;
	    QListWidget*    listWidget;
    QPalette        paletteGray;
    QPalette        paletteWhite;
    QMenu*          listContextMenu;

    std::weak_ptr<StockmanNamespace::StockmanSpace::DBManager> dbManager;
    std::shared_ptr<StockmanNamespace::AppContext> context_;

public:
    std::vector<Util::ConnectionInfo> KelpieList;
    QDialog*                     ConnectDialog  = nullptr;
    bool                         tryConnect     = false;
    bool                         isNewProfile   = false;
    bool                         FromAction     = false;

    void setupUi( QDialog* Form );
    Util::ConnectionInfo StartDialog( bool FromAction );
    void passDB( const std::shared_ptr<StockmanNamespace::StockmanSpace::DBManager>& dbm );
    void setContext( std::shared_ptr<StockmanNamespace::AppContext> ctx ) { context_ = std::move(ctx); }
    bool connectGrpcBackend();

private slots:
            void onButton_Connect();
    void onButton_NewProfile();

    void itemSelected();
    void handleContextMenu(const QPoint &pos);

    void itemRemove();
    void itemsClear();
};

#endif
