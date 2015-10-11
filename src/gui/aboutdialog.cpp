#include "aboutdialog.h"
#include "ui_aboutdialog.h"

AboutDialog::AboutDialog(QWidget *parent) :
    QDialog(parent),
    ui(new Ui::AboutDialog)
{
    ui->setupUi(this);
    QString customText;
    customText += "Built on " + QString::fromLocal8Bit(__DATE__);
    ui->labelCustom->setText(customText);
}

AboutDialog::~AboutDialog()
{
    delete ui;
}
