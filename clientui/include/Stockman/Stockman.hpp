#ifndef STOCKMAN_STOCKMAN_HPP
#define STOCKMAN_STOCKMAN_HPP

#include <QMainWindow>
#include <memory>

#include <Stockman/AppContext.hpp>
#include <UserInterface/StockmanUI.hpp>
#include <Stockman/DBManager/DBManager.hpp>

using namespace StockmanNamespace;

class StockmanSpace::Stockman {

public:
    UserInterface::StockmanUi StockmanAppUI;
    std::shared_ptr<DBManager> dbManager;
    std::shared_ptr<StockmanNamespace::AppContext> context_;

    explicit Stockman( QMainWindow* );
    Stockman( std::shared_ptr<StockmanNamespace::AppContext> context, QMainWindow* );
    ~Stockman();

    Stockman(const Stockman&) = delete;
    Stockman& operator=(const Stockman&) = delete;
    Stockman(Stockman&&) = delete;
    Stockman& operator=(Stockman&&) = delete;

    void Init( int argc, char** argv );
    void Start();
    void RefreshTheme();

    static void Exit();

private:
    std::unique_ptr<QMainWindow> mainWindow_;

    QMainWindow* window() const { return mainWindow_.get(); }
};

#endif
