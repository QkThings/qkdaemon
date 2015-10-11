#include "mainwindow.h"
#include "ui_mainwindow.h"
#include "qkdaemon.h"
#include "qkconnworker.h"
#include "aboutdialog.h"

#include <QDebug>
#include <QListWidget>
#include <QTime>
#include <QMessageBox>
#include <QUuid>
#include <QHeaderView>

MainWindow::MainWindow(QWidget *parent) :
    QMainWindow(parent),
    ui(new Ui::MainWindow)
{
    ui->setupUi(this);
    setWindowTitle("QkDaemon");

    _qkdaemon = new QkDaemon(this);
    connect(&_qkdaemon->connManager, SIGNAL(message(int,QString)),
            this, SLOT(slotMessage(int,QString)));

#ifdef Q_OS_UNIX
    ui->textMessages->setFont(QFont("Monospace", 9));
#endif
#ifdef Q_OS_WIN
    ui->listConn->setFont(QFont("Consolas"));
    ui->textMessages->setFont(QFont("Consolas", 9));
#endif

    QHeaderView *hh = ui->listConn->horizontalHeader();
    hh->setSectionResizeMode(0, QHeaderView::ResizeToContents);
    hh->setSectionResizeMode(1, QHeaderView::ResizeToContents);
    hh->setSectionResizeMode(2, QHeaderView::Fixed);

    ui->listConn->setColumnWidth(2, 120);


    connect(ui->buttonSerialReload, SIGNAL(clicked()),
            this, SLOT(slotReloadSerial()));
    connect(ui->buttonAddConn, SIGNAL(clicked()),
            this, SLOT(slotAddConnection()));
    connect(ui->buttonMessages, SIGNAL(clicked()),
            this, SLOT(slotShowHideMessages()));
    connect(ui->buttonAbout, SIGNAL(clicked()),
            this, SLOT(slotAbout()));

    connect(ui->comboConnType, SIGNAL(currentIndexChanged(int)),
            this, SLOT(updateInterface()));
    connect(ui->comboSerialPort, SIGNAL(currentIndexChanged(int)),
            this, SLOT(updateInterface()));
/*
    _qkconnectWorker = new QkConnWorker(this);
    connect(_qkconnectWorker, SIGNAL(availableSerialPorts(QStringList)),
            this, SLOT(setAvailableSerial(QStringList)));
    connect(_qkconnectWorker, SIGNAL(message(int,QString)),
            this, SLOT(slotMessage(int,QString)));
*/

    setAvailableSerial(QkConnWorker::listSerialPorts());
    slotMessage(QkDaemon::MESSAGE_INFO, "QkConnect v" + QkConnWorker::version());

    updateInterface();
}

MainWindow::~MainWindow()
{
    delete ui;
}

void MainWindow::slotConnect()
{

}

void MainWindow::slotAddConnection()
{
    int connType = ui->comboConnType->currentIndex();
    QMap<QString, QString> params;
    int connID;
    int spyPort;

    if(connType == ctSerialPort)
    {        
        connType = (int) QkConnWorker::ctSerialPort;
        params["portname"] = ui->comboSerialPort->currentText();
        params["baudrate"] = ui->comboSerialBaud->currentText();
        params["dtr"] = "1";
    }

    if(_qkdaemon->connManager.add(connType, params, &connID))
    {
        spyPort = _qkdaemon->connManager.worker(connID)->port() + 1;
        QString connStr = QString().sprintf(" %04X %5d", connID, spyPort);
        connStr += QString(" %1 %2 %3")
                .arg(ui->comboConnType->currentText())
                .arg(ui->comboSerialPort->currentText())
                .arg(ui->comboSerialBaud->currentText());



//        QString connStr = QString().sprintf(" %6s %5d %-12s %-16s %s",
//                                            QString().sprintf("%06X", connID),
//                                            spyPort,
//                                            ui->comboConnType->currentText().toStdString().c_str(),
//                                            ui->comboSerialPort->currentText().toStdString().c_str(),
//                                            ui->comboSerialBaud->currentText().toStdString().c_str());

//        ui->listConn->addItem(connStr);
        QString connIDStr = QString().sprintf("%04X", connID);
        QString spyPortStr = QString().sprintf("%d", spyPort);
        QString connArgsStr = QString("%1 %2")
                .arg(ui->comboSerialPort->currentText())
                .arg(ui->comboSerialBaud->currentText());

        int r = ui->listConn->rowCount();
        ui->listConn->insertRow(r);
        ui->listConn->setItem(r, 0, new QTableWidgetItem(connIDStr));
        ui->listConn->setItem(r, 1, new QTableWidgetItem(spyPortStr));
        ui->listConn->setItem(r, 2, new QTableWidgetItem(ui->comboConnType->currentText()));
        ui->listConn->setItem(r, 3, new QTableWidgetItem(connArgsStr));
    }
    else
    {
        qDebug() << "failed to add conn";
    }


}

void MainWindow::slotRemoveConnection()
{
    int curRow = ui->listConn->currentRow();
    if(curRow != -1)
    {
//        delete ui->listConn->takeItem(curRow);
    }
}

void MainWindow::slotMessage(int type, QString text)
{
    QString msg;
    msg += "(" + QTime::currentTime().toString("hh:mm:ss") + ") ";

    switch(type)
    {
    case QkDaemon::MESSAGE_INFO: msg += "[i] "; break;
    case QkDaemon::MESSAGE_ERROR: msg += "[e] "; break;
    case QkDaemon::MESSAGE_DEBUG: msg += "[d] "; break;
    }

    msg += text;

    ui->textMessages->append(msg);
}

void MainWindow::slotShowHideMessages()
{
    ui->textMessages->setVisible(!ui->textMessages->isVisible());
}

void MainWindow::slotReloadSerial()
{
    setAvailableSerial(QkConnWorker::listSerialPorts());
}

void MainWindow::setAvailableSerial(QStringList portNames)
{
    ui->comboSerialPort->clear();
    ui->comboSerialPort->addItems(portNames);
}

void MainWindow::updateInterface()
{
    QString connType = ui->comboConnType->currentText().toLower();
    if(connType == "serial")
    {
        QString portName = ui->comboSerialPort->currentText();
        ui->buttonAddConn->setEnabled(!portName.isEmpty());
    }

    int curRow = ui->listConn->currentRow();
    ui->buttonRemoveConn->setEnabled(curRow != -1);
}

void MainWindow::slotAbout()
{
    AboutDialog aboutDialog;
    aboutDialog.exec();
}
