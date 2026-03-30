#ifndef STOCKMAN_ABOUTDIALOG_H
#define STOCKMAN_ABOUTDIALOG_H

#include <QDialog>
#include <QGridLayout>
#include <QLabel>
#include <QPushButton>
#include <QSpacerItem>
#include <QTextBrowser>

class About : public QDialog
{
private:
    QGridLayout*    gridLayout;
    QLabel*         label;
    QPushButton*    pushButton;
    QSpacerItem*    horizontalSpacer;
    QTextBrowser*   textBrowser;

public:
    QDialog *AboutDialog;

    void setupUi();
    About( QDialog* );

public slots:
    void onButtonClose();
};

#endif
