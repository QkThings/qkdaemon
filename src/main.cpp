#include <QApplication>
#include <QDate>
#include "mainwindow.h"
#include "qkgui.h"

int main(int argc, char *argv[])
{
    QApplication a(argc, argv);
    QCoreApplication::setApplicationName("QkDaemon");
    QCoreApplication::setApplicationVersion(QDate::fromString(__DATE__,"MMM dd yyyy").toString("yyyyMMdd"));
    QkGUI::qt_fusionDark();

    MainWindow w;
    w.show();

    return a.exec();
}
