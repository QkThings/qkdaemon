#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
class QkDaemon;

namespace Ui {
class MainWindow;
}

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    explicit MainWindow(QWidget *parent = 0);
    ~MainWindow();

private slots:
    void slotConnect();
    void slotAddConnection();
    void slotRemoveConnection();
    void slotReloadSerial();
    void slotMessage(int type, QString text);
    void slotShowHideMessages();
    void slotAbout();

    void setAvailableSerial(QStringList portNames);
    void updateInterface();

private:
    enum ConnType
    {
        ctSerialPort
    };

    Ui::MainWindow *ui;
    QkDaemon *_qkdaemon;
};

#endif // MAINWINDOW_H
